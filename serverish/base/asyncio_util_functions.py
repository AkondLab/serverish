import asyncio
import warnings


async def wait_for_psce(fut, timeout):
    """Deprecated: use ``asyncio.timeout()``. Kept for API compatibility.

    The name meant "prevent silent cancel error": a workaround for an
    ``asyncio.wait_for()`` defect of old Python versions, where a cancellation
    arriving together with the result was lost. ``asyncio.timeout()`` has no
    such defect::

        async with asyncio.timeout(timeout):
            result = await fut

    :param fut: awaitable to wait for
    :param timeout: seconds, or None for no limit
    """
    warnings.warn("wait_for_psce() is deprecated: use 'async with asyncio.timeout(...)'",
                  DeprecationWarning, stacklevel=2)
    async with asyncio.timeout(timeout):
        return await fut
