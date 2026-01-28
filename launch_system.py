import subprocess
import time
import sys

def run_command(command, wait=True):
    print(f"\n>>> {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"ERREUR: {result.stderr}")
    
    if wait:
        time.sleep(2)
    
    return result.returncode

print("=== LANCEMENT DU SYSTÈME BIG DATA ===")

print("\n1. Demarrage Docker Kafka...")
run_command("cd docker && docker-compose up -d")

time.sleep(10)

print("\n2. Configuration Kafka...")
run_command("python setup_kafka.py")

print("\n3. Demarrage Spark (dans 5 secondes)...")
print("   OUVREZ UNE NOUVELLE FENÊTRE POWERSHELL ET EXÉCUTEZ:")
print("   cd \"C:\\Users\\hp\\Documents\\Projet_BigData_IoT_Resultat_Final\"")
print("   python src\\spark\\spark_simple.py")

time.sleep(5)

print("\n4. Demarrage Producteur (dans 5 secondes)...")
print("   OUVREZ UNE AUTRE FENÊTRE ET EXÉCUTEZ:")
print("   cd \"C:\\Users\\hp\\Documents\\Projet_BigData_IoT_Resultat_Final\"")
print("   python simple_producer.py")

print("\n5. Verification...")
time.sleep(10)

print("\n=== INSTRUCTIONS ===")
print("Ouvrez 4 fenêtres PowerShell:")
print("1. docker-compose up -d")
print("2. python setup_kafka.py")
print("3. python src\\spark\\spark_simple.py")
print("4. python simple_producer.py")
print("\nAttendez entre chaque étape.")
