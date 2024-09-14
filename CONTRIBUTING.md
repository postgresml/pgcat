## Introduction

Thank you for contributing! Just a few tips here:

1. `cargo fmt` and `cargo clippy` your code before opening up a PR
2. Run the test suite (e.g. `pgbench`) to make sure everything still works. The tests are in `.circleci/run_tests.sh`.
3. Performance is important, make sure there are no regressions in your branch vs. `main`.

## How to run the integration tests locally and iterate on them
We have integration tests written in Ruby, Python, Go and Rust.
Below are the steps to run them in a developer-friendly way that allows iterating and quick turnaround.
Hear me out, this should be easy, it will involve opening a shell into a container with all the necessary dependancies available for you and you can modify the test code and immediately rerun your test in the interactive shell.


Quite simply, make sure you have docker installed and then run
`./start_test_env.sh`

That is it!

Within this test environment you can modify the file in your favorite IDE and rerun the tests without having to bootstrap the entire environment again.

Once the environment is ready, you can run the tests by running
Ruby:   `cd /app/tests/ruby && bundle exec ruby <test_name>.rb --format documentation`
Python: `cd /app/ && pytest`
Rust:   `cd /app/tests/rust && cargo run`
Go:     `cd /app/tests/go && /usr/local/go/bin/go test`

You can also rebuild PgCat directly within the environment and the tests will run against the newly built binary
To rebuild PgCat, just run `cargo build` within the container under `/app`

![Animated gif showing how to run tests](https://github.com/user-attachments/assets/2258fde3-2aed-4efb-bdc5-e4f12dcd4d33)



Happy hacking!

## TODOs

See [Issues]([url](https://github.com/levkk/pgcat/issues)).
