import typer


def main():
    pass

from typing import Optional
import asyncio
import logging
from pathlib import Path

from serverish.services.server import run
from serverish.services.servers.simple import SimpleServer

app = typer.Typer(
    help="Serverish Microservices Server CLI - service management and control tool",
    add_completion=False)

servers_app = typer.Typer(help="Server operations")
app.add_typer(servers_app, name="server")


@servers_app.command("run")
def run_server(
        server_type: str = typer.Option("simple", "--type", "-t", help="Server type (simple, fastapi)"),
        config: Optional[Path] = typer.Option(None, "--config", "-c", help="Server config file path"),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
):
    """Run a server instance"""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level)

    server_config = None
    if config:
        import yaml
        with open(config) as f:
            server_config = yaml.safe_load(f)

    if server_type == "simple":
        try:
            asyncio.run(run(server_config))
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(f'Error: {e}')
            raise typer.Exit(1)
    elif server_type == "fastapi":
        typer.echo("FastAPI server support coming soon")
        raise typer.Exit(1)
    else:
        typer.echo(f"Unknown server type: {server_type}")
        raise typer.Exit(1)


@servers_app.command("list")
def list_servers():
    """List running server instances"""
    typer.echo("Server listing functionality coming soon")


@servers_app.command("stop")
def stop_server(
        server_id: str = typer.Argument(..., help="Server ID to stop")
):
    """Stop a running server instance"""
    typer.echo("Server stop functionality coming soon")


@app.command("version")
def version():
    """Show version information"""
    from serverish import __version__
    typer.echo(f"Serverish version: {__version__}")


def main():
    app()


if __name__ == "__main__":
    main()