from __future__ import annotations
from typing import Any, Dict, Tuple
from .schema import ContainerConfig


def _nanoseconds(seconds: int) -> int:
    """Docker принимает длительности в наносекундах."""
    return int(seconds * 1_000_000_000)


def build_docker_payload(cfg: ContainerConfig) -> Tuple[Dict[str, Any], str | None, str | None]:
    """
    Возвращает кортеж:
      (payload для тела запроса, name для query-параметра, network_name)
    payload соответствует схеме POST /containers/create
    """

    try:
        exposed: Dict[str, Dict] = {}
        bindings: Dict[str, list] = {}
        for p in cfg.ports:
            key = f"{p.container_port}/{p.protocol}"
            exposed[key] = {}
            bindings.setdefault(key, []).append(
                {"HostIp": p.host_ip, "HostPort": str(p.host_port)}
            )

        env_list = [f"{k}={v}" for k, v in cfg.env.items()]

        mounts = []
        for m in cfg.mounts:
            entry: Dict[str, Any] = {"Type": m.type, "Target": m.target}
            if m.source:
                entry["Source"] = m.source
            if m.read_only:
                entry["ReadOnly"] = True
            mounts.append(entry)

        host_config: Dict[str, Any] = {
            "PortBindings": bindings,
            "RestartPolicy": {
                "Name": cfg.restart_policy.name,
                "MaximumRetryCount": cfg.restart_policy.maximum_retry_count,
            },
            "AutoRemove": cfg.auto_remove,
            "Privileged": cfg.privileged,
            "ReadonlyRootfs": cfg.read_only_rootfs,
            "Mounts": mounts,
        }

        if cfg.network_mode:
            host_config["NetworkMode"] = cfg.network_mode
        if cfg.dns:
            host_config["Dns"] = cfg.dns
        if cfg.stop_timeout is not None:
            host_config["StopTimeout"] = cfg.stop_timeout

        r = cfg.resources
        if r.cpu_shares is not None:
            host_config["CpuShares"] = r.cpu_shares
        if r.memory is not None:
            host_config["Memory"] = r.memory
        if r.memory_swap is not None:
            host_config["MemorySwap"] = r.memory_swap
        if r.nano_cpus is not None:
            host_config["NanoCpus"] = r.nano_cpus
        if r.pids_limit is not None:
            host_config["PidsLimit"] = r.pids_limit

        healthcheck = None
        if cfg.healthcheck:
            healthcheck = {
                "Test": cfg.healthcheck.test,
                "Interval": _nanoseconds(cfg.healthcheck.interval),
                "Timeout": _nanoseconds(cfg.healthcheck.timeout),
                "Retries": cfg.healthcheck.retries,
                "StartPeriod": _nanoseconds(cfg.healthcheck.start_period),
            }

        payload: Dict[str, Any] = {
            "Image": cfg.image,
            "Env": env_list,
            "Labels": cfg.labels,
            "ExposedPorts": exposed,
            "HostConfig": host_config,
            "Tty": cfg.tty,
            "OpenStdin": cfg.stdin_open,
            "AttachStdout": False,
            "AttachStderr": False,
        }

        if cfg.cmd is not None:
            payload["Cmd"] = cfg.cmd
        if cfg.entrypoint is not None:
            payload["Entrypoint"] = cfg.entrypoint
        if cfg.hostname:
            payload["Hostname"] = cfg.hostname
        if cfg.working_dir:
            payload["WorkingDir"] = cfg.working_dir
        if cfg.user:
            payload["User"] = cfg.user
        if healthcheck:
            payload["Healthcheck"] = healthcheck

        if not env_list:
            payload.pop("Env")
        if not cfg.labels:
            payload.pop("Labels")
        if not exposed:
            payload.pop("ExposedPorts")
        if not mounts:
            host_config.pop("Mounts")
        if not bindings:
            host_config.pop("PortBindings")

        return payload, cfg.name, cfg.network_name
    except Exception as e:
        print(e)
        return {}, "", ""