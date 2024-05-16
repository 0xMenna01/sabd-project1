FROM python:3.12

# Update packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*
# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Create a directory for the application
WORKDIR /app

COPY requirements.txt .
# Install virtualenv and create a virtual environment
RUN pip install virtualenv && \
    virtualenv venv && \
    .venv/bin/activate && \
    pip install -r requirements.txt

# Copy the remaining files to the working directory
COPY src/ .
COPY config/ .

# Command to run the application
CMD ["/bin/bash"]
