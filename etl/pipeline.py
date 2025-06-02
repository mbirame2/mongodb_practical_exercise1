import pandas as pd
import pymongo
from datetime import datetime
import os
import shutil
from pymongo.errors import PyMongoError
from bson import ObjectId

# Constants (configurable)
SOURCE_DIR = "../data/"
DESTINATION_DIR = "../alreadyUpload/"
MONGO_URI = "mongodb://mongo:mongo@localhost:27017/"
DB_NAME = "nyc"
COLLECTION_NAME = "medical"

files_already_upload = []

def mongotransformDf(file_path):

    try:
        return pd.read_csv(file_path, sep=';', on_bad_lines='skip')
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def dfToJson(row):

    now = datetime.now()
    return {
        "patient": {
            "_id": ObjectId(), 
            "name": row.get('Name'),
            "age": row.get('Age'),
            "gender": row.get('Gender'),
            "blood_type": row.get('Blood Type'),
            "medical_info": {
                "condition": row.get('Medical Condition'),
                "admission_date": row.get('Date of Admission'),
                "discharge_date": row.get('Discharge Date'),
                "doctor": row.get('Doctor'),
                "medication": row.get('Medication'),
                "test_results": row.get('Test Results')
            },
            "hospital_details": {
                "hospital": row.get('Hospital'),
                "room_number": row.get('Room Number'),
                "admission_type": row.get('Admission Type')
            },
            "financial_info": {
                "insurance_provider": row.get('Insurance Provider'),
                "billing_amount": row.get('Billing Amount')
            },
            "last_updated": now
        }
    }

def process_file(file):

    file_path = os.path.join(SOURCE_DIR, file)
    try:

        if file in files_already_upload:
            print(f"Skipping already processed file: {file}")
            return


        df = mongotransformDf(file_path)
        if df is None or df.empty:
            print(f"No data in {file}")
            return


        documents = [dfToJson(row.to_dict()) for _, row in df.iterrows()]


        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        coll = db[COLLECTION_NAME]
        coll.insert_many(documents)


        files_already_upload.append(file)
        shutil.move(file_path, os.path.join(DESTINATION_DIR, file))
        print(coll)

    except PyMongoError as e:
        print(f"MongoDB error for {file}: {e}")
    except Exception as e:
        print(f"Unexpected error processing {file}: {e}")

def mongoQueris():

    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://mongo:mongo@localhost:27017/")
    db = client["nyc"]
    coll = db["medical"]
    
    def print_separator(title):
        print(f"\n{'=' * 50}")
        print(f"{title.upper():^50}")
        print(f"{'=' * 50}\n")
    
    # 1. Total patients
    print_separator("total patients")
    total_patients = coll.count_documents({})
    print(f"Total patients in collection: {total_patients:,}\n")
    
    # 2. Patients admitted after Jan 1, 2023
    print_separator("patients admitted after jan 1, 2023")
    admitted_after = coll.find({
        "patient.medical_info.admission_date": {"$gt": "01/01/2023"}
    }).limit(5)
    print("First 5 patients:")
    for i, patient in enumerate(admitted_after, 1):
        print(f"{i}. {patient['patient']['name']} (Admitted: {patient['patient']['medical_info']['admission_date']})")
    
    # 3. Patients older than 50
    print_separator("patients older than 50")
    older_than_50 = coll.count_documents({"patient.age": {"$gt": 50}})
    print(f"Total patients >50 years old: {older_than_50:,}\n")
    
    # 4. Patients named "Thomas"
    print_separator("patients named thomas")
    thomas_patients = coll.find({
        "patient.name": {"$regex": "Thomas", "$options": "i"}
    }).limit(5)
    print("First 5 matching patients:")
    for i, patient in enumerate(thomas_patients, 1):
        print(f"{i}. {patient['patient']['name']} (Age: {patient['patient']['age']})")
    
    # 5. Group by Medical Condition
    print_separator("patients per medical condition")
    condition_counts = coll.aggregate([
        {"$group": {"_id": "$patient.medical_info.condition", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    print("Top 5 conditions:")
    for i, cond in enumerate(condition_counts, 1):
        print(f"{i}. {cond['_id']}: {cond['count']:,} patients")
    
    # 6. Medication frequency
    print_separator("medication frequency")
    medication_freq = coll.aggregate([
        {"$group": {"_id": "$patient.medical_info.medication", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ])
    print("Top 5 medications:")
    for i, med in enumerate(medication_freq, 1):
        print(f"{i}. {med['_id']}: {med['count']:,} patients")
    
    # 7. Patients taking Lipitor
    print_separator("patients taking lipitor")
    lipitor_patients = coll.find({
        "patient.medical_info.medication": "Lipitor"
    }).limit(5)
    print("First 5 patients taking Lipitor:")
    for i, patient in enumerate(lipitor_patients, 1):
        print(f"{i}. {patient['patient']['name']} (Condition: {patient['patient']['medical_info']['condition']})")
    
    client.close()

def main():

    
    files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.csv')] 
    print(files)
    
    for file in files:
        process_file(file)

    mongoQueris()
if __name__ == "__main__":
    main()