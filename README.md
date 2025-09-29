Requirements: docker

Run docker, then docker compose up --build to build the application.

Initial success scenario is already implemented.

To run the tests, create a virtual environment, activate it and run python3 test/test_microservices.py

Practical task is implement the failure scenario. What happens if the stock or payment event fails? If either fails the order status needs to be updated to rejected, and the operation of the another needs to be rolled back, but only if that event was successfull.

A theoretical question is what would you do to scale the consumers?
