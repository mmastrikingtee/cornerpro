if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-ahead", type=int, default=120)
    ap.add_argument("--max-events", type=int, default=5)
    args = ap.parse_args()
    ingest(days_ahead=args.days_ahead, max_events=args.max_events)