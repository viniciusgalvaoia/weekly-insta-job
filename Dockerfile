FROM python:3.9-slim

# Set environment variables
WORKDIR workspace

# Install base requirements
ADD requirements/* requirements/
RUN pip install -r requirements/run.txt

# https://github.com/pyupio/safety
RUN pip install safety

# Set arguments
ARG env="dev"
ARG full_env="development"
ARG job_name

# Set environment variables
ENV PD_PYTHON_ENV $full_env

# Copy the job folder
ADD . .

CMD python -m job
