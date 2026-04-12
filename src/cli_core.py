import importlib
import pkgutil
import commands
import functools

COMMANDS = {}

def command(name: str = None):
    def decorator(func):
        cli_name = name or func.__name__.replace("_", "-")

        if cli_name in COMMANDS:
            raise ValueError(
                f"CLI command '{cli_name}' is already registered "
                f"by function '{COMMANDS[cli_name].__name__}'"
            )

        @functools.wraps(func)
        def wrapper(args):
            # unpack the list into separate args
            return func(*args)

        COMMANDS[cli_name] = wrapper
        return func
    return decorator

def load_commands():
    """Dynamically import all modules under commands/ so they register themselves."""
    for _, module_name, _ in pkgutil.iter_modules(commands.__path__):
        importlib.import_module(f"{commands.__name__}.{module_name}")


def start_main_cli_loop(do_loop = True):
    load_commands()

    if do_loop:
        print("\n\nWelcome to Virtual Museum CLI!")
        print("type help for list of available commands")

    while do_loop:
        try:

            cmd_input = input("\n> ").strip()

            if not cmd_input:
                continue

            if cmd_input.lower() in {"quit", "exit"}:
                break

            parts = cmd_input.split()
            cmd_name, args = parts[0], parts[1:]

            if cmd_name in COMMANDS:
                COMMANDS[cmd_name](args)

            else:
                print(f"Unknown command: {cmd_name}")

        except Exception as e:
            print(f"Error: {e}")




@command("help")  # force CLI command name to "help"
def cli_help():
    """Show available commands."""
    print("Available commands:")
    for name, func in COMMANDS.items():
        doc = func.__doc__ or "No description"
        print(f"  {name}: {doc}")

    print(f"\n\n  type exit or quit to exit")
