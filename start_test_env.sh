GREEN="\033[0;32m"
RED="\033[0;31m"
BLUE="\033[0;34m"
RESET="\033[0m"


cd tests/docker/
docker compose kill main || true
docker compose build main
docker compose down
docker compose up -d
# wait for the container to start
while ! docker compose exec main ls; do
    echo "Waiting for test environment to start"
    sleep 1
done
echo "==================================="
docker compose exec -e LOG_LEVEL=error -d main toxiproxy-server
docker compose exec --workdir /app main cargo build
docker compose exec -d --workdir /app main ./target/debug/pgcat ./.circleci/pgcat.toml
docker compose exec --workdir /app/tests/ruby main bundle install
docker compose exec --workdir /app/tests/python main pip3 install -r requirements.txt
echo "Interactive test environment ready"
echo "To run integration tests, you can use the following commands:"
echo -e "   ${BLUE}Ruby:   ${RED}cd /app/tests/ruby && bundle exec ruby tests.rb --format documentation${RESET}"
echo -e "   ${BLUE}Python: ${RED}cd /app && python3 tests/python/tests.py${RESET}"
echo -e "   ${BLUE}Rust:   ${RED}cd /app/tests/rust && cargo run ${RESET}"
echo -e "   ${BLUE}Go:     ${RED}cd /app/tests/go && /usr/local/go/bin/go test${RESET}"
echo "the source code for tests are directly linked to the source code in the container so you can modify the code and run the tests again"
echo "You can rebuild PgCat from within the container by running" 
echo -e "  ${GREEN}cargo build${RESET}"
echo "and then run the tests again"
echo "==================================="
docker compose exec --workdir /app/tests main bash
