import pennylane as qml
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

class QuantumRecommender:
    def __init__(self, user_data_path, movie_data_path, user_profile_path, n_qubits=8):
        """
        Initialize the quantum recommender system.
        
        Args:
            user_data_path: Path to user viewing data
            movie_data_path: Path to movie data
            user_profile_path: Path to user profile data
            n_qubits: Number of qubits to use for the quantum circuit
        """
        self.n_qubits = n_qubits
        self.movies_df = pd.read_csv(movie_data_path)
        self.user_viewing_df = pd.read_csv(user_data_path)
        self.user_profiles_df = pd.read_csv(user_profile_path)
        
        # Preprocess data
        self._preprocess_data()
        
        # Set up quantum device
        self.dev = qml.device("default.qubit", wires=self.n_qubits)
        
        # Define quantum circuit
        self.circuit = qml.QNode(self.quantum_circuit, self.dev)
        
    def _preprocess_data(self):
        """Preprocess and normalize data for quantum processing."""
        # Normalize movie features
        numeric_cols = ['release_year', 'rating', 'popularity', 'runtime']
        self.scaler = MinMaxScaler()
        self.movies_df[numeric_cols] = self.scaler.fit_transform(self.movies_df[numeric_cols])
        
        # One-hot encode genre
        self.genres = self.movies_df['genre'].unique()
        for genre in self.genres:
            self.movies_df[f'genre_{genre}'] = (self.movies_df['genre'] == genre).astype(int)
            
        # Add is_original as a feature
        self.movies_df['is_original'] = self.movies_df['is_original'].astype(int)
        
        # Create user-movie interaction matrix
        self.user_movie_matrix = pd.pivot_table(
            self.user_viewing_df, 
            values='rating', 
            index='user_id', 
            columns='movie_id',
            fill_value=0
        )
    
    def quantum_circuit(self, features, weights):
        """
        Quantum circuit for feature embedding and processing.
        
        Args:
            features: Movie or user features to embed
            weights: Trainable weights for the circuit
        
        Returns:
            Expectation values of the qubits
        """
        # Embed the classical features into quantum states
        for i, feat in enumerate(features):
            qml.RY(np.pi * feat, wires=i % self.n_qubits)
            
        # Apply parameterized quantum circuit
        for layer in range(2):  # 2-layer circuit
            # Entangling layer
            for i in range(self.n_qubits - 1):
                qml.CNOT(wires=[i, i + 1])
            qml.CNOT(wires=[self.n_qubits - 1, 0])
            
            # Rotation layer with weights
            for i in range(self.n_qubits):
                qml.RX(weights[layer, i, 0], wires=i)
                qml.RY(weights[layer, i, 1], wires=i)
                qml.RZ(weights[layer, i, 2], wires=i)
                
        # Measure all qubits
        return [qml.expval(qml.PauliZ(i)) for i in range(self.n_qubits)]
    
    def get_movie_features(self, movie_id):
        """Get normalized features for a movie."""
        movie = self.movies_df[self.movies_df['movie_id'] == movie_id]
        if movie.empty:
            raise ValueError(f"Movie ID {movie_id} not found")
            
        # Extract relevant features
        features = []
        
        # Add normalized numeric features
        features.extend(movie[['release_year', 'rating', 'popularity', 'runtime']].values[0])
        
        # Add genre one-hot encoding
        for genre in self.genres:
            features.append(movie[f'genre_{genre}'].values[0])
            
        # Add is_original feature
        features.append(movie['is_original'].values[0])
        
        return np.array(features)
    
    def get_user_features(self, user_id):
        """Get user features based on viewing history and profile."""
        user_profile = self.user_profiles_df[self.user_profiles_df['user_id'] == user_id]
        if user_profile.empty:
            raise ValueError(f"User ID {user_id} not found")
            
        # Get user age normalized
        age_norm = (user_profile['age'].values[0] - 18) / 82  # Assuming age range 18-100
        
        # Get preferred genre encoding
        preferred_genre = user_profile['preferred_genre'].values[0]
        genre_encoding = [1 if genre == preferred_genre else 0 for genre in self.genres]
        
        # Get subscription type (0 for standard, 1 for premium)
        subscription = 1 if user_profile['subscription_type'].values[0] == 'premium' else 0
        
        # Get user's average rating
        user_ratings = self.user_viewing_df[self.user_viewing_df['user_id'] == user_id]
        avg_rating = user_ratings['rating'].mean() / 5  # Normalize to [0,1]
        
        # Get watch completion rate
        completion_rate = user_ratings['completed'].mean()
        
        # Combine all features
        features = [age_norm, avg_rating, completion_rate, subscription]
        features.extend(genre_encoding)
        
        return np.array(features)
    
    def compute_quantum_similarity(self, user_id, movie_id):
        """Compute quantum similarity between a user and a movie."""
        # Get features
        user_features = self.get_user_features(user_id)
        movie_features = self.get_movie_features(movie_id)
        
        # Pad or truncate features to match n_qubits
        if len(user_features) > self.n_qubits:
            user_features = user_features[:self.n_qubits]
        elif len(user_features) < self.n_qubits:
            user_features = np.pad(user_features, (0, self.n_qubits - len(user_features)))
            
        if len(movie_features) > self.n_qubits:
            movie_features = movie_features[:self.n_qubits]
        elif len(movie_features) < self.n_qubits:
            movie_features = np.pad(movie_features, (0, self.n_qubits - len(movie_features)))
        
        # Initialize random weights for the quantum circuit
        weights = np.random.uniform(0, 2*np.pi, size=(2, self.n_qubits, 3))
        
        # Get quantum embeddings
        user_embedding = self.circuit(user_features, weights)
        movie_embedding = self.circuit(movie_features, weights)
        
        # Compute similarity
        similarity = np.dot(user_embedding, movie_embedding) / (np.linalg.norm(user_embedding) * np.linalg.norm(movie_embedding))
        
        return (similarity + 1) / 2  # Normalize to [0,1]
    
    def generate_recommendations(self, user_id, top_n=10, exclude_watched=True):
        """
        Generate movie recommendations for a user.
        
        Args:
            user_id: User ID to generate recommendations for
            top_n: Number of recommendations to generate
            exclude_watched: Whether to exclude already watched movies
            
        Returns:
            DataFrame with recommended movies and similarity scores
        """
        # Get list of movies user has already watched
        watched_movies = self.user_viewing_df[self.user_viewing_df['user_id'] == user_id]['movie_id'].unique()
        
        # Find candidate movies
        if exclude_watched:
            candidate_movies = self.movies_df[~self.movies_df['movie_id'].isin(watched_movies)]['movie_id'].unique()
        else:
            candidate_movies = self.movies_df['movie_id'].unique()
            
        # Compute quantum similarity for each candidate movie
        similarities = []
        for movie_id in candidate_movies:
            similarity = self.compute_quantum_similarity(user_id, movie_id)
            similarities.append({'movie_id': movie_id, 'similarity': similarity})
            
        # Create recommendations dataframe and sort by similarity
        recs_df = pd.DataFrame(similarities)
        recs_df = recs_df.sort_values('similarity', ascending=False).head(top_n)
        
        # Join with movie data to get movie details
        recs_df = recs_df.merge(self.movies_df[['movie_id', 'title', 'genre', 'rating']], on='movie_id')
        
        return recs_df


# Alternative implementations for situations where quantum computing may not be available
class ClassicalRecommender:
    def __init__(self, user_data_path, movie_data_path, user_profile_path):
        """Initialize with classical collaborative filtering."""
        self.movies_df = pd.read_csv(movie_data_path)
        self.user_viewing_df = pd.read_csv(user_data_path)
        self.user_profiles_df = pd.read_csv(user_profile_path)
        
        # Create user-movie interaction matrix
        self.user_movie_matrix = pd.pivot_table(
            self.user_viewing_df, 
            values='rating', 
            index='user_id', 
            columns='movie_id',
            fill_value=0
        )
        
        # Compute similarity between users
        self.user_similarity = cosine_similarity(self.user_movie_matrix)
        self.user_similarity_df = pd.DataFrame(
            self.user_similarity,
            index=self.user_movie_matrix.index,
            columns=self.user_movie_matrix.index
        )
    
    def generate_recommendations(self, user_id, top_n=10, exclude_watched=True):
        """Generate recommendations using collaborative filtering."""
        # Get list of movies user has already watched
        watched_movies = self.user_viewing_df[self.user_viewing_df['user_id'] == user_id]['movie_id'].unique()
        
        # Get similar users
        similar_users = self.user_similarity_df[user_id].sort_values(ascending=False).iloc[1:6]
        
        # Get movies these similar users have rated highly
        similar_user_ratings = self.user_viewing_df[
            (self.user_viewing_df['user_id'].isin(similar_users.index)) & 
            (self.user_viewing_df['rating'] >= 4)
        ]
        
        # Exclude movies the user has already watched if required
        if exclude_watched:
            similar_user_ratings = similar_user_ratings[~similar_user_ratings['movie_id'].isin(watched_movies)]
        
        # Count recommendations and get top n
        movie_recs = similar_user_ratings['movie_id'].value_counts().sort_values(ascending=False).head(top_n)
        
        # Create dataframe with recommendations
        recs_df = pd.DataFrame({'movie_id': movie_recs.index, 'rec_count': movie_recs.values})
        
        # Join with movie data
        recs_df = recs_df.merge(self.movies_df[['movie_id', 'title', 'genre', 'rating']], on='movie_id')
        
        return recs_df 