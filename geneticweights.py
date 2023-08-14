import boto3
import pandas as pd
import numpy as np
import time
import os

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def preprocess_data(data):
    data['Vessel Density'] = data[1] / data[4]
    data['Slip Scarcity'] = data[1] / data[3]
    data['Relative Population Growth'] = data[5] / data[4]
    data['Home Value to Income Ratio'] = data[6] / data[7]
    data['5yr Vessel Registration CAGR'] = data[2]
    return data.iloc[:, -4:]

def compute_fitness(data, weights):
    scores = data.dot(weights)
    return -np.var(scores)

def mutate(child):
    mutation_strength = 0.1
    mutation = (np.random.random(size=child.shape) - 0.5) * mutation_strength
    child += mutation
    child = np.clip(child, 0, 1)
    return child / child.sum()

def crossover(parent1, parent2):
    alpha = np.random.random()
    child1 = alpha * parent1 + (1 - alpha) * parent2
    child2 = (1 - alpha) * parent1 + alpha * parent2
    return child1, child2

def genetic_algorithm(data, generations=100, population_size=100, elite_size=10, tournament_size=5):
    population = [np.random.dirichlet(np.ones(data.shape[1])) for _ in range(population_size)]
    for _ in range(generations):
        fitnesses = [compute_fitness(data, individual) for individual in population]
        ranked_parents = [x for _, x in sorted(zip(fitnesses, population), key=lambda pair: pair[0])]
        
        new_population = []
        for i in range(elite_size):
            new_population.append(ranked_parents[i])
        
        while len(new_population) < population_size:
            tourney_candidates = np.random.choice(population, size=tournament_size, replace=False)
            parent1, parent2 = tourney_candidates[0], tourney_candidates[1]
            child1, child2 = crossover(parent1, parent2)
            child1 = mutate(child1)
            child2 = mutate(child2)
            new_population.extend([child1, child2])
        
        population = new_population
    
    return ranked_parents[0]

def main():
    obj = s3.get_object(Bucket=BUCKET_NAME, Key='countydata.csv')
    data = pd.read_csv(obj['Body'], header=None).iloc[:, 1:]
    data = preprocess_data(data)
    start_time = time.time()
    best_weights = genetic_algorithm(data.to_numpy())
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    metric_titles = ['Vessel Density', 'Slip Scarcity', 'Relative Population Growth', 'Home Value to Income Ratio', '5yr Vessel Registration CAGR']
    
    output = "\n".join([f"{metric_titles[i]}: {best_weights[i]}" for i in range(len(best_weights))])
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    output += f"\n\nTimestamp: {timestamp}\nTime elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s"
    
    with open('weights.txt', 'w') as f:
        f.write(output)
    
    s3.upload_file('weights.txt', BUCKET_NAME, 'weights.txt')
    print("Optimization completed and weights saved to AWS.")

if __name__ == '__main__':
    main()
