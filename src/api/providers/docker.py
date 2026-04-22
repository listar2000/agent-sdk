"""Docker provider — NotImplementedError stubs for Phase 0."""


async def create_volume(name: str) -> str:
    raise NotImplementedError("docker not yet supported")


async def delete_volume(ref: str) -> None:
    raise NotImplementedError("docker not yet supported")


async def get_sandbox_status(ref: str) -> str:
    raise NotImplementedError("docker not yet supported")


async def start_sandbox(ref: str) -> None:
    raise NotImplementedError("docker not yet supported")


async def destroy_sandbox(inst) -> None:
    raise NotImplementedError("docker not yet supported")


async def stop_sandbox(inst) -> None:
    raise NotImplementedError("docker not yet supported")


async def ensure_supervisor_url(inst, **kw) -> str:
    raise NotImplementedError("docker not yet supported")


async def install_supervisor(volume_ref: str, agent_type: str) -> None:
    raise NotImplementedError("docker not yet supported")


async def volume_tree(ref: str, path: str) -> str:
    raise NotImplementedError("docker not yet supported")


async def volume_read(ref: str, path: str) -> bytes:
    raise NotImplementedError("docker not yet supported")


async def volume_write(ref: str, path: str, content: bytes) -> None:
    raise NotImplementedError("docker not yet supported")
