from __future__ import annotations
from typing import Optional, Literal, List, Dict, Union
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
import re


class PortBinding(BaseModel):
    """Проброс портов: host_port -> container_port/proto"""
    host_ip: str = "0.0.0.0"
    host_port: int = Field(..., ge=1, le=65535)
    container_port: int = Field(..., ge=1, le=65535)
    protocol: Literal["tcp", "udp", "sctp"] = "tcp"


class Mount(BaseModel):
    """Монтирование тома/бинда"""
    type: Literal["bind", "volume", "tmpfs", "npipe"] = "bind"
    source: Optional[str] = None
    target: str
    read_only: bool = False

    @model_validator(mode="after")
    def check_source(self):
        if self.type in ("bind", "volume") and not self.source:
            raise ValueError(f"Для типа '{self.type}' обязателен 'source'")
        if not self.target.startswith("/"):
            raise ValueError("target должен быть абсолютным путём внутри контейнера")
        return self


class RestartPolicy(BaseModel):
    name: Literal["no", "always", "unless-stopped", "on-failure"] = "no"
    maximum_retry_count: int = Field(0, ge=0)


class HealthcheckConfig(BaseModel):
    test: Union[str, List[str]] = Field(
        ..., description="Команда, строка или список (CMD/CMD-SHELL)"
    )
    interval: int = Field(30, ge=1)
    timeout: int = Field(5, ge=1)
    retries: int = Field(3, ge=1)
    start_period: int = Field(0, ge=0)

    @field_validator("test")
    @classmethod
    def normalize_test(cls, v):
        if isinstance(v, str):
            return ["CMD-SHELL", v]
        if isinstance(v, list) and v and v[0] in ("CMD", "CMD-SHELL", "NONE"):
            return v
        raise ValueError("test должен быть строкой или списком, начинающимся с CMD/CMD-SHELL")


class ResourceLimits(BaseModel):
    cpu_shares: Optional[int] = Field(None, ge=2, le=262144)
    memory: Optional[int] = Field(None, ge=6 * 1024 * 1024)
    memory_swap: Optional[int] = Field(None, ge=-1)
    nano_cpus: Optional[int] = Field(None, ge=1_000_000)
    pids_limit: Optional[int] = Field(None, ge=-1)



NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")


class ContainerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image: str = Field(..., min_length=1)

    name: Optional[str] = None
    hostname: Optional[str] = None
    cmd: Optional[List[str]] = None
    entrypoint: Optional[List[str]] = None
    env: Dict[str, str] = Field(default_factory=dict)
    labels: Dict[str, str] = Field(default_factory=dict)
    working_dir: Optional[str] = None
    user: Optional[str] = None

    ports: List[PortBinding] = Field(default_factory=list)
    network_mode: Optional[str] = None   # bridge / host / none / container:<id>
    network_name: Optional[str] = None
    dns: List[str] = Field(default_factory=list)

    mounts: List[Mount] = Field(default_factory=list)

    restart_policy: RestartPolicy = Field(default_factory=RestartPolicy)
    healthcheck: Optional[HealthcheckConfig] = None
    tty: bool = False
    stdin_open: bool = False
    privileged: bool = False
    auto_remove: bool = False
    read_only_rootfs: bool = False
    stop_timeout: Optional[int] = Field(default=None, ge=0)

    resources: ResourceLimits = Field(default_factory=ResourceLimits)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        if v is None:
            return v
        if not NAME_RE.match(v):
            raise ValueError(
                "Имя контейнера должно начинаться с буквы/цифры и содержать "
                "только [a-zA-Z0-9_.-]"
            )
        return v

    @field_validator("env", mode="before")
    @classmethod
    def normalize_env(cls, v):
        """Разрешаем передавать как список 'KEY=VALUE' или dict."""
        if isinstance(v, list):
            result = {}
            for item in v:
                if "=" not in item:
                    raise ValueError(f"Переменная окружения '{item}' должна быть в формате KEY=VALUE")
                k, val = item.split("=", 1)
                result[k] = val
            return result
        return v

    @model_validator(mode="after")
    def check_conflicts(self):
        if self.network_mode == "host" and self.ports:
            raise ValueError("При network_mode='host' проброс портов не поддерживается")

        # при auto_remove нельзя restart policy, кроме 'no'
        if self.auto_remove and self.restart_policy.name != "no":
            raise ValueError("auto_remove=True несовместим с политикой перезапуска")

        seen = set()
        for p in self.ports:
            key = (p.host_ip, p.host_port, p.protocol)
            if key in seen:
                raise ValueError(f"Дубликат публикации порта: {key}")
            seen.add(key)

        return self