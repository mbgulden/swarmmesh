import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="swarmmesh CLI")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("status", help="Show mesh status")
    subparsers.add_parser("peers", help="List peers")
    subparsers.add_parser("topics", help="List topics")
    
    publish_parser = subparsers.add_parser("publish", help="Publish message")
    publish_parser.add_argument("topic", help="Topic to publish to")
    publish_parser.add_argument("message", help="Message payload")

    args = parser.parse_args()

    if args.command == "status":
        print("Mesh status: OK")
    elif args.command == "peers":
        print("Known peers: []")
    elif args.command == "topics":
        print("Active topics: []")
    elif args.command == "publish":
        print(f"Published to {args.topic}: {args.message}")
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
