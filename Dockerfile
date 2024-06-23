# Use the official Python image from Docker Hub
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy the script and .env file into the container
COPY script.py .
COPY hashjoin_v1.py .
COPY hashjoin_v2.py .
COPY semi_join.py .

COPY .env .
COPY dataset /app/dataset

# Run the script
CMD ["python", "script.py"]
