# Use the official Python image from Docker Hub
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy the script and .env file into the container
COPY script.py .
COPY .env .

# Run the script
CMD ["python", "script.py"]
