Build/Run Commands

To build script using dockerfile:
docker build -t script .

To run script using dockerfile:
docker run --rm -v $(pwd):/app script /app/fl_1.csv "First Name,Last Name"

To build tests from dockerfile: 
docker build -t spark-test .

To run test from dockerfile:
docker run --rm spark-test
