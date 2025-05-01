import csv

def read_secrets_from_csv(filename):
    secrets = {}
    
    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                id = row['id']
                secret_key = row['secret_key']
                secrets[id] = secret_key
                
    except FileNotFoundError:
        print(f"Error: The file {filename} does not exist.")
    except KeyError as e:
        print(f"Error: Missing column in CSV: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return secrets

