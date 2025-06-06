# Step 1: Use an official Python runtime as a parent image
# We'll use a slim version of Python 3.11 for a smaller image size.
FROM python:3.11-slim

# Step 2: Set the working directory inside the container
WORKDIR /app

# Step 3: Install necessary system dependencies (if any)
# This app doesn't have many, but this is good practice.
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Step 4: Copy the requirements file and install Python packages
# This step ensures we only re-install packages when the requirements file
# changes, which speeds up future builds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Copy the rest of the application code into the container
# This includes main.py, index.html, and the Banners folder.
COPY . .

# Step 6: Expose the port the app runs on
EXPOSE 8000

# Step 7: Define the command to run your app using uvicorn
# Note: reload is turned off for production.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
