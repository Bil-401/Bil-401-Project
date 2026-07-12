import os
import subprocess
import sys

# Configure environment variables for Spark on Windows
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
os.environ["HADOOP_HOME"] = r"c:\Bil-401-Project\hadoop"
os.environ["PATH"] = r"c:\Bil-401-Project\hadoop\bin;" + os.environ["PATH"]

# Set PYSPARK executables
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Determine SPARK_HOME dynamically
try:
    import pyspark
    spark_home = os.path.dirname(pyspark.__file__)
    os.environ["SPARK_HOME"] = spark_home
    print(f"Dynamically set SPARK_HOME to: {spark_home}")
except ImportError:
    print("Warning: pyspark is not installed in the current environment.")

# Set working directory to the project folder
cwd = r"c:\Bil-401-Project\BIL401_PROJECT"

# List of scripts to run in order
scripts = [
    r"src\01_data_ingestion.py",
    r"src\02_feature_engineering.py",
    r"src\03_model_training.py",
    r"src\04_inference.py",
    r"src\05_visualization.py"
]

print("Starting pipeline execution...")

for script in scripts:
    script_path = os.path.join(cwd, script)
    print(f"\n=========================================")
    print(f"Running {script}...")
    print(f"=========================================")
    
    # Run using the current python executable
    result = subprocess.run([sys.executable, script_path], cwd=cwd)
    
    if result.returncode != 0:
        print(f"\nError: {script} failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    else:
        print(f"\nSuccess: {script} completed successfully.")

print("\nPipeline completed successfully!")
