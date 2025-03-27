# Quantum Recommender System for Movies and TV Shows

A Netflix-style recommender system powered by quantum computing algorithms.

## Overview

This project demonstrates how quantum computing can be applied to recommendation systems. It uses quantum circuits to compute similarity between users and content, providing personalized recommendations with a Netflix-like user interface.

## Features

- **Quantum-Powered Recommendations**: Utilizes PennyLane for quantum computing to generate personalized recommendations.
- **Netflix-Style UI**: Modern, responsive interface inspired by Netflix.
- **Classical Fallback**: Falls back to classical recommendation algorithms when quantum computing is not available.
- **User Profiles**: Switch between different user profiles to see different recommendations.
- **Detailed Content View**: Click on content to see detailed information and quantum-match statistics.

## Technologies Used

- **Backend**:
  - Python Flask for the web server
  - PennyLane for quantum computing
  - Pandas and NumPy for data processing
  - Scikit-learn for classical ML algorithms

- **Frontend**:
  - HTML5, CSS3, and JavaScript
  - Responsive design

## How It Works

### Quantum Recommendation Algorithm

The system uses a hybrid quantum-classical approach:

1. **Data Preprocessing**: User and movie features are normalized and encoded.
2. **Quantum Feature Embedding**: Features are embedded into quantum states.
3. **Parameterized Quantum Circuit**: A variational quantum circuit processes these embeddings.
4. **Similarity Calculation**: Quantum state measurements are used to calculate similarity between users and content.
5. **Ranking**: Content is ranked by quantum similarity scores for personalized recommendations.

### Classical Fallback

If quantum computing is not available, the system falls back to:

- Collaborative filtering based on user similarity
- Content-based filtering based on movie features

## Setup and Installation

1. Install Python 3.8+ and required packages:
   ```
   pip install -r requirements.txt
   ```

2. Run the application:
   ```
   python app.py
   ```

3. Access the application at http://localhost:5000

## Project Structure

```
quantum_recommender/
├── app/
│   ├── quantum/
│   │   ├── __init__.py
│   │   └── recommender.py
│   ├── static/
│   │   ├── css/
│   │   │   └── styles.css
│   │   └── js/
│   │       └── main.js
│   ├── templates/
│   │   ├── base.html
│   │   └── index.html
│   ├── __init__.py
│   └── routes.py
├── data/
│   ├── movies.csv
│   ├── user_profiles.csv
│   └── user_viewing.csv
├── app.py
├── requirements.txt
└── README.md
```

## Sample Users

The system includes 5 sample users with different preferences:

- **User 1**: Science Fiction fan
- **User 2**: Drama enthusiast
- **User 3**: Crime drama viewer
- **User 4**: Sci-Fi and Fantasy viewer
- **User 5**: Action/Adventure fan

## Future Improvements

- Implement more advanced quantum circuits for better recommendations
- Add real-time quantum circuit visualization
- Integrate with real streaming content APIs
- Expand the dataset with more movies and TV shows
- Implement quantum-inspired optimization techniques for faster recommendations

## License

MIT License 