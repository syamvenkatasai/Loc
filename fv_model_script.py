import json
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import joblib

# Function to read data from a JSON file
def read_corpus_from_json(json_file):
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Define the path to the JSON file containing your corpus
json_file_path = r'/var/www/prediction_api/app/data/ambanketrade/invoice_fv.json'

# Read the corpus from the JSON file
corpus = read_corpus_from_json(json_file_path)

# Split data into features (X) and labels (y)
X = [text for text, label in corpus]
y = [label for text, label in corpus]

# Text preprocessing: Convert text to a bag of words (BoW) representation
vectorizer = CountVectorizer()
X = vectorizer.fit_transform(X)

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train the Logistic Regression classifier
clf = LogisticRegression()
clf.fit(X_train, y_train)

# Make predictions on the test data
y_pred = clf.predict(X_test)

# Evaluate the classifier
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

# Print classification report (includes precision, recall, F1-score, and support)
report = classification_report(y_test, y_pred, zero_division=1)
print("Classification Report:\n", report)

# Save the trained model and vectorizer for later inference
model_filename = r'/var/www/prediction_api/app/data/ambanketrade/invoice_fv_logistic_regression_model.joblib'
vectorizer_filename = r'/var/www/prediction_api/app/data/ambanketrade/invoice_fv_count_vectorizer.joblib'

# Save the model and vectorizer to files
joblib.dump(clf, model_filename)
joblib.dump(vectorizer, vectorizer_filename)
print("Model files saved successfully.")