Requirements: docker

Run docker, then docker compose up --build to build the application.




To run the tests, 
create a virtual environment: python -m venv venv
activate your venv (differs from OS)
download dependencies: pip install -r requirements.txt


activate it and run python3 test/test_microservices.py

Initial success scenario is already implemented. Last test will fail as that has not been implemented yet.


Practical task is implement the failure scenario. What happens if the stock or payment event fails? If either fails the order status needs to be updated to rejected, and the operation of the another needs to be rolled back, but only if that event was successfull.

A theoretical question is what would you do to scale the consumers?
