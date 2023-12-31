import boto3
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
import matplotlib.pyplot as plt
from itertools import combinations

# Using Heroku environment variables for AWS credentials
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)


def preprocess_data(data):
  data['Vessel Density'] = data['# of registered vessels'] / data[
    'county population']
  data['Slip Scarcity'] = data['# of registered vessels'] / data[
    '# of slips per county']
  data['Relative Population Growth'] = data[
    'county population growth (over 10 years)'] / data['county population']
  data['Home Value to Income Ratio'] = data['median home value'] / data[
    'average household income']

  data.drop(columns=[
    'county name', '# of registered vessels', '# of slips per county',
    'county population', 'county population growth (over 10 years)',
    'median home value', 'average household income'
  ],
            inplace=True)

  data = (data - data.mean()) / data.std()

  return data


def compute_gradient(data, weights):
  scores = np.dot(data, weights)
  gradient = -2 * np.dot(data.T, scores - np.mean(scores))
  return gradient


def project_to_simplex(v):
  """Project vector v to the simplex"""
  n_features = v.shape[0]
  u = np.sort(v)[::-1]
  cssv = np.cumsum(u) - 1
  ind = np.arange(n_features) + 1
  cond = u - cssv / ind > 0
  rho = np.count_nonzero(cond)
  theta = cssv[rho - 1] / float(rho)
  w = np.maximum(v - theta, 0)
  return w


def gradient_descent(data,
                     initial_weights=None,
                     learning_rate=0.01,
                     decay_rate=0.95,
                     max_iterations=5000,
                     tolerance=1e-6):
  if initial_weights is None:
    initial_weights = np.ones(data.shape[1]) / data.shape[1]
  weights = initial_weights

  for i in range(max_iterations):
    gradient = compute_gradient(data, weights)
    weights -= learning_rate * gradient

    # Projecting the weights to the simplex
    weights = project_to_simplex(weights)

    learning_rate *= decay_rate
    if np.linalg.norm(gradient) < tolerance:
      break

  return weights


def multiple_starts_gd(data, num_starts=10000):  # Changed from 100 to 10,000
  accumulated_weights = np.zeros(data.shape[1])

  for _ in range(num_starts):
    initial_weights = np.random.dirichlet(np.ones(data.shape[1]))
    weights = gradient_descent(data, initial_weights)
    accumulated_weights += weights

  return accumulated_weights / num_starts


def plot_3d_combinations(data):
  variables = data.columns
  for comb in combinations(variables, 3):
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    x, y, z = comb
    ax.scatter(data[x], data[y], data[z], c='r', marker='o')

    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.set_zlabel(z)

    filename = f"{x}_{y}_{z}.png"
    plt.savefig(filename)
    s3.upload_file(filename, BUCKET_NAME, filename)
    os.remove(filename)
    plt.close()


def optimize_weights():
  obj = s3.get_object(Bucket=BUCKET_NAME, Key='countydata.csv')
  data = pd.read_csv(obj['Body'],
                     header=None,
                     names=[
                       'county name', '# of registered vessels',
                       '5 year cagr for registered vessels',
                       '# of slips per county', 'county population',
                       'county population growth (over 10 years)',
                       'median home value', 'average household income'
                     ])

  data = preprocess_data(data)

  start_time = time.time()
  weights = multiple_starts_gd(data.to_numpy())
  elapsed_time = time.time() - start_time

  hours, remainder = divmod(elapsed_time, 3600)
  minutes, seconds = divmod(remainder, 60)

  titles = [
    'Vessel Density', 'Slip Scarcity', 'Relative Population Growth',
    'Home Value to Income Ratio', '5 year cagr for registered vessels'
  ]

  with open('weights.txt', 'w') as f:
    for title, weight in zip(titles, weights):
      f.write(f"{title}: {weight}\n")
    f.write(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Time taken: {int(hours)}h {int(minutes)}m {int(seconds)}s\n")

  s3.upload_file('weights.txt', BUCKET_NAME, 'weights.txt')
  print('Optimization completed and weights saved to AWS.')


if __name__ == '__main__':
  optimize_weights()
