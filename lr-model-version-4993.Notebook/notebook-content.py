# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "event_stream": {
# META       "known_event_streams": [
# META         {
# META           "artifact_id": "b0e13ab4-1c91-45a0-9a3f-ab3eaa9c3c0f",
# META           "stream_id": "b0e13ab4-1c91-45a0-9a3f-ab3eaa9c3c0f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Track Machine Learning experiments and models
# 
# A machine learning model is a file that has been trained to recognize certain types of patterns. You train a model over a set of data, providing it an algorithm that it can use to reason over and learn from those data. Once you have trained the model, you can use it to reason over data that it hasn't seen before, and make predictions about that data.
# 
# In this notebook, you will learn the basic steps to run an experiment, add a model version to track run metrics and parameters and register a model.


# CELL ********************

import mlflow

# Set given experiment as the active experiment. If an experiment with this name does not exist, a new experiment with this name is created.
mlflow.set_experiment(<"your-experiment-name">)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow.sklearn
import numpy as np
from sklearn.linear_model import LogisticRegression
from mlflow.models.signature import infer_signature

# Start your training job with `start_run()`
with mlflow.start_run() as run:

    lr = LogisticRegression()
    X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
    y = np.array([0, 0, 1, 1, 1, 0])
    lr.fit(X, y)
    score = lr.score(X, y)
    signature = infer_signature(X, y)

    # Activate the MLFlow logging API to log your training job metrics
    print("test log_metrics.")
    mlflow.log_metric("score", score)

    # Activate the MLFlow logging API to log your training job parameters
    print("test log_params.")
    mlflow.log_param("alpha", "alpha")

    # Activate the MLFlow logging API to log your model artifacts
    print("test log_model.")
    mlflow.sklearn.log_model(lr, "market_analysis", signature=signature)
    print("Model saved in run_id=%s" % run.info.run_id)
    
    # Register the model produced from your training job.
    print("test register_model.")
    mlflow.register_model(
        "runs:/{}/market_analysis".format(run.info.run_id), "market_analysis"
    )
    print("All done")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
