import asyncio
import logging

from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.console import Console

from serverish.messenger import Messenger
from serverish.messenger.msg_progress_pub import get_progresspublisher, MsgProgressPublisher
from serverish.messenger.msg_progress_read import get_progressreader

logging.basicConfig(level=logging.INFO,  handlers=[RichHandler()])


# console = Console()

async def task_sim_rich(progress: Progress, sec: int) -> None:
    """ Simulates a task that takes `sec` seconds to complete, reports progress to `rich.progress`"""

    task_id = progress.add_task(f"{sec}s task", total=sec)
    dsec = 10 * sec
    for i in range(1, dsec + 10):
        await asyncio.sleep(0.1)
        progress.update(task_id, advance=0.1)
    logging.info(f"Task {task_id} finished!")

async def task_sim_messenger(progress: MsgProgressPublisher, sec: int) -> None:
    """ Simulates a task that takes `sec` seconds to complete, reports progress to `MsgProgressPublisher`"""

    task_id = await progress.add_task(f"{sec}s task", total=sec) # ! await
    dsec = 10 * sec
    for i in range(1, dsec + 10):
        await asyncio.sleep(0.1)
        await progress.update(task_id, advance=0.1)  # ! await
    logging.info(f"Task {task_id} finished!")


async def progress_classic_demo():
    """Simulates tasks and displays progress using `rich.progress` directly"""
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        TimeRemainingColumn(),
        # transient=True,
    ) as progress:
        await asyncio.gather(
            task_sim_rich(progress, 3),
            task_sim_rich(progress, 4),
            task_sim_rich(progress, 10),
            task_sim_rich(progress, 5),
        )


async def progress_messenger_demo():
    """Simulates tasks and displays progress using `rich.progress` via `ProgressPublisher`"""

    async def progress_publischer(subject):
        progress = get_progresspublisher(subject)
        async with progress:
            await asyncio.gather(
                task_sim_messenger(progress, 3),
                task_sim_messenger(progress, 4),
                task_sim_messenger(progress, 10),
                task_sim_messenger(progress, 5),
            )

    async def progress_subscriber(subject):
        sub = get_progressreader(subject)

        with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=None),
                "[progress.percentage]{task.percentage:>3.0f}%",
                "•",
                TimeRemainingColumn(),
                # transient=True,
        ) as progress:
            tasks = {}
            async for msg, meta in sub:
                if msg.id not in tasks:
                    tasks[msg.id] = progress.add_task(msg.description, id=msg.id, total=msg.total)
                progress.update(tasks[msg.id], completed=msg.completed)

    subject = 'test.example.rich_progress'

    async with Messenger().context(host='localhost', port=4222):
        await asyncio.gather(
            progress_publischer(subject),
            progress_subscriber(subject),
    )


async def main():
    # logging.info('Direct progress demo')
    # await progress_classic_demo()
    logging.info('Messenger progress demo')
    await progress_messenger_demo()

if __name__ == "__main__":
    asyncio.run(main())
