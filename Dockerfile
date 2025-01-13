# Use the official Python 3.10 base image
FROM python:3.10

# Set the working directory in the container to /app
WORKDIR /piima

# Copy the current directory contents into the container at /app
COPY . /piima

# Install any required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port your app runs on (optional, assuming it's 3131 based on your example)
EXPOSE 3132

# Run main.py when the container launches
CMD ["python", "main.py"] 