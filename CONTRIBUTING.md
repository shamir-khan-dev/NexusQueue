# Contributing to NexusQueue

Thank you for your interest in contributing to NexusQueue!

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/shamir-khan-dev/NexusQueue.git
   cd NexusQueue
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Run tests:**
   ```bash
   npm test
   ```

4. **Start the local cluster and live dashboard:**
   ```bash
   npm start
   ```

5. **Run the high-throughput benchmark:**
   ```bash
   npm run benchmark
   ```

## Pull Request Guidelines

- Ensure all 22 unit & integration tests pass with `npm test`.
- Add new test cases in `tests/unit/` or `tests/integration/` for any new features or bug fixes.
- Verify TypeScript compiles cleanly with `npm run build`.
- Maintain clean, descriptive commit messages adhering to Conventional Commits (e.g. `feat: ...`, `fix: ...`).

## License

By contributing to NexusQueue, you agree that your contributions will be licensed under the [MIT License](LICENSE).
