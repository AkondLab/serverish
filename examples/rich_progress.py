import asyncio
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console

console = Console()

async def symuluj_task(progress, task_id: int, sekundy: int) -> None:
    for i in range(1, sekundy + 1):
        await asyncio.sleep(1)
        progress.update(task_id, advance=1)
    console.log(f"Task {task_id} zakończony!")

async def main() -> None:
    total_seconds = 5
    with Progress(
        TextColumn("[bold blue]{task.fields[id]}"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        TimeRemainingColumn(),
        transient=True,
    ) as progress:

        task1 = progress.add_task("[cyan]Task 1", id="T1", total=total_seconds)
        task2 = progress.add_task("[green]Task 2", id="T2", total=total_seconds)
        task3 = progress.add_task("[magenta]Task 3", id="T3", total=total_seconds)

        await asyncio.gather(
            symuluj_task(progress, task1, 3),
            symuluj_task(progress, task2, 4),
            symuluj_task(progress, task3, 5),
        )

if __name__ == "__main__":
    asyncio.run(main())
