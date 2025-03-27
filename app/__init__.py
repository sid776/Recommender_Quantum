from flask import Flask
from app.quantum.recommender import ClassicalRecommender

# Check if quantum modules are available
try:
    import pennylane as qml
    from app.quantum.recommender import QuantumRecommender
    using_quantum = True
except ImportError:
    using_quantum = False

# Define Recommender class using the available recommenders
class Recommender:
    def __init__(self):
        # Define paths to data files
        data_dir = 'data'
        user_data_path = f'{data_dir}/user_viewing.csv'
        movie_data_path = f'{data_dir}/movies.csv'
        user_profile_path = f'{data_dir}/user_profiles.csv'
        
        # Initialize the appropriate recommender
        if using_quantum:
            try:
                self.recommender = QuantumRecommender(
                    user_data_path=user_data_path,
                    movie_data_path=movie_data_path,
                    user_profile_path=user_profile_path
                )
                print("Initialized quantum recommender")
            except Exception as e:
                print(f"Failed to initialize quantum recommender: {e}")
                self.recommender = ClassicalRecommender(
                    user_data_path=user_data_path,
                    movie_data_path=movie_data_path,
                    user_profile_path=user_profile_path
                )
                print("Fell back to classical recommender")
        else:
            self.recommender = ClassicalRecommender(
                user_data_path=user_data_path,
                movie_data_path=movie_data_path,
                user_profile_path=user_profile_path
            )
            print("Initialized classical recommender")
        
        # Store dataframes from the recommender for easy access
        self.movies_df = self.recommender.movies_df
        self.user_viewing_df = self.recommender.user_viewing_df
        self.user_profiles_df = self.recommender.user_profiles_df
    
    def generate_recommendations(self, user_id, top_n=10):
        """Generate recommendations for a user"""
        return self.recommender.generate_recommendations(user_id, top_n=top_n)

# Initialize recommender as a global variable
recommender = Recommender()

def create_app():
    app = Flask(__name__)
    app.secret_key = 'quantum_recommender_secret_key'  # Add a secret key for sessions
    
    # Register blueprints
    from app.routes import main_bp
    app.register_blueprint(main_bp)
    
    return app 