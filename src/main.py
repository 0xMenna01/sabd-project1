from cli.cli import Cli
import dotenv


def main() -> None:
    # Load environment variables
    dotenv.load_dotenv()
    # Start the CLI
    Cli().start()


if __name__ == "__main__":
    main()
