import boto3
import pandas as pd
import numpy as np
import time
import os  # for accessing environment variables

# Fetching the AWS info from Heroku environment variables
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def preprocess_data(data):
    data['Vessel Density'] = data.iloc[:, 1] / data.iloc[:, 4]
    data['Slip Scarcity'] = data.iloc[:, 1] / data.iloc[:, 3]
    data['Relative Population Growth'] = data.iloc[:, 5] / data.iloc[:, 4]
    data['Home Value to Income Ratio'] = data.iloc[:, 6] / data.iloc[:, 7]
    data['Vessel Registration Growth'] = data.iloc[:, 2]
    return data[['Vessel Density', 'Slip Scarcity', 'Relative Population Growth', 'Home Value to Income Ratio', 'Vessel Registration Growth']]

def compute_gradient(data, weights):
    scores = np.dot(data, weights)
    gradient = -2 * np.dot(data.T, scores - np.mean(scores))
    return gradient

def gradient_descent(data, initial_weights=None, learning_rate=0.01, decay_rate=0.95, max_iterations=1000, tolerance=1e-6):
    if initial_weights is None:
        initial_weights = np.ones(data.shape[1]) / data.shape[1]
    weights = initial_weights
    
    for i in range(max_iterations):
        gradient = compute_gradient(data, weights)
        weights += learning_rate * gradient
        learning_rate *= decay_rate
        if np.linalg.norm(gradient) < tolerance:
            break
    
    return weights

def multiple_starts_gd(data, num_starts=10):
    best_weights = None
    best_variance = float('-inf')
    
    for _ in range(num_starts):
        initial_weights = np.random.dirichlet(np.ones(data.shape[1]))
        weights = gradient_descent(data, initial_weights)
        
        scores = data.dot(weights)
        variance = np.var(scores)
        
        if variance > best_variance:
            best_variance = variance
            best_weights = weights
            
    return best_weights

def optimize_weights():
    obj = s3.get_object(Bucket=BUCKET_NAME, Key='countydata.csv')
    data = pd.read_csv(obj['Body'], header=None)
    
    data_processed = preprocess_data(data)
    
    start_time = time.time()
    weights = multiple_starts_gd(data_processed.to_numpy())
    elapsed_time = time.time() - start_time

    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    with open('weights.txt', 'w') as f:
        for weight in weights:
            f.write(f"{weight}\n")
    
    s3.upload_file('weights.txt', BUCKET_NAME, 'weights.txt')
    
    print({'message': f'Optimization completed and weights saved to AWS. Time taken: {int(hours)}h {int(minutes)}m {int(seconds)}s'})

if __name__ == '__main__':
    optimize_weights()
