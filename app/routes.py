from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from app import recommender, using_quantum

main_bp = Blueprint('main', __name__)

# The recommender is now imported from app/__init__.py, so we don't need to initialize it here

@main_bp.route('/')
def index():
    """Render the main page or redirect to onboarding if new user."""
    print("Session contents:", session)
    print("User ID in session:", session.get('user_id'))
    
    # Clear cookies if requested - helpful for testing
    if request.args.get('clear_session'):
        session.clear()
        return redirect(url_for('main.index'))
    
    if 'user_id' not in session:
        print("No user_id in session, redirecting to onboarding")
        return redirect(url_for('main.onboarding'))
    
    print(f"User ID {session['user_id']} found in session, showing main page")
    return render_template('index.html', using_quantum=using_quantum)

@main_bp.route('/onboarding')
def onboarding():
    """Show the onboarding questionnaire for new users."""
    return render_template('onboarding.html', using_quantum=using_quantum)

@main_bp.route('/create_profile', methods=['POST'])
def create_profile():
    """Process the onboarding form and create a user profile."""
    try:
        # Get demographic data
        age = int(request.form.get('age'))
        gender = request.form.get('gender', 'O')
        location = request.form.get('location', 'Global')
        occupation = request.form.get('occupation', 'other')
        
        # Get genre preferences
        preferred_genre = request.form.get('preferred_genre')
        
        # Get genre ratings
        genre_ratings = {}
        for genre in ['scifi', 'drama', 'comedy', 'action', 'thriller', 'horror', 'fantasy', 'romance']:
            rating_key = f'genre_{genre}'
            if rating_key in request.form:
                genre_ratings[genre] = int(request.form.get(rating_key))
        
        # Get content themes
        themes = request.form.getlist('themes')
        
        # Get viewing habits
        watch_habit = request.form.get('watch_habit', 'casual')
        viewing_time = request.form.get('viewing_time', 'evening')
        session_duration = int(request.form.get('session_duration', 120))
        
        # Get favorite shows
        favorites = request.form.getlist('favorites')
        
        # Generate a new user ID (max existing + 1)
        max_user_id = recommender.user_profiles_df['user_id'].max()
        new_user_id = int(max_user_id) + 1
        
        # Create new user profile
        new_profile = {
            'user_id': new_user_id,
            'age': age,
            'gender': gender,
            'country': location,
            'preferred_genre': preferred_genre,
            'subscription_type': 'premium'  # Default
        }
        
        # Save extended profile data as JSON
        extended_profile = {
            'occupation': occupation,
            'genre_ratings': genre_ratings,
            'themes': themes,
            'watch_habit': watch_habit,
            'viewing_time': viewing_time,
            'session_duration': session_duration
        }
        
        # Create profiles directory if it doesn't exist
        os.makedirs('data/profiles', exist_ok=True)
        
        # Save extended profile as JSON
        with open(f'data/profiles/user_{new_user_id}.json', 'w') as f:
            json.dump(extended_profile, f)
        
        # Append to user_profiles.csv using pandas concat instead of append
        profiles_df = recommender.user_profiles_df
        new_profile_df = pd.DataFrame([new_profile])
        profiles_df = pd.concat([profiles_df, new_profile_df], ignore_index=True)
        profiles_df.to_csv('data/user_profiles.csv', index=False)
        
        # Create viewing history based on favorites
        viewing_df = recommender.user_viewing_df
        viewing_records = []
        
        # Weight the favorites by their order of selection
        for i, movie_id in enumerate(favorites):
            # Get movie details
            movie = recommender.movies_df[recommender.movies_df['movie_id'] == int(movie_id)]
            if not movie.empty:
                # Create viewing record with higher ratings for first selections
                # This simulates the user liking their first choices more
                rating = min(5, max(3, 5 - (i // 2)))
                
                # Create viewing record
                viewing_record = {
                    'user_id': new_user_id,
                    'movie_id': int(movie_id),
                    'rating': rating,
                    'completed': 1,  # Assume they finished it
                    'date_watched': (datetime.now() - pd.Timedelta(days=np.random.randint(1, 30))).strftime('%Y-%m-%d')
                }
                viewing_records.append(viewing_record)
        
        if viewing_records:
            new_viewing_df = pd.DataFrame(viewing_records)
            viewing_df = pd.concat([viewing_df, new_viewing_df], ignore_index=True)
            viewing_df.to_csv('data/user_viewing.csv', index=False)
        
        # Update the recommender with new data
        recommender.user_profiles_df = profiles_df
        recommender.user_viewing_df = viewing_df
        
        # Store user ID in session
        session['user_id'] = new_user_id
        
        # Also store profile details in session
        session['watch_habit'] = watch_habit
        session['viewing_time'] = viewing_time
        session['preferred_genre'] = preferred_genre
        
        # Success response
        return jsonify({
            'success': True,
            'user_id': new_user_id,
            'redirect': url_for('main.index')
        })
    
    except Exception as e:
        print(f"Error creating profile: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@main_bp.route('/login', methods=['POST'])
def login():
    """Handle user login."""
    data = request.get_json()
    user_id = int(data.get('user_id', 1))  # Default to user 1 if not specified
    
    session['user_id'] = user_id
    
    return jsonify({'success': True})

@main_bp.route('/recommendations')
def get_recommendations():
    """Get recommendations for the current user."""
    user_id = session.get('user_id', 1)  # Default to user 1 if not in session
    
    try:
        # Check if user exists in profiles
        if user_id not in recommender.user_profiles_df['user_id'].values:
            print(f"User {user_id} not found, showing popular content instead")
            # Return popular movies instead
            popular_movies = recommender.movies_df.sort_values('popularity', ascending=False).head(10)
            recommendations = []
            for _, row in popular_movies.iterrows():
                rec = {
                    'id': int(row['movie_id']),
                    'title': row['title'],
                    'genre': row['genre'],
                    'rating': float(row['rating'])
                }
                # Generate a popularity score in the same range as similarity
                rec['similarity'] = float(row['popularity']) / 100
                recommendations.append(rec)
        else:
            # Get personalized recommendations
            recs_df = recommender.generate_recommendations(user_id, top_n=15)  # Get more than we need for filtering
            
            # Convert to list of dictionaries for JSON response
            recommendations = []
            for _, row in recs_df.iterrows():
                rec = {
                    'id': int(row['movie_id']),
                    'title': row['title'],
                    'genre': row['genre'],
                    'rating': float(row['rating'])
                }
                if 'similarity' in row:
                    rec['similarity'] = float(row['similarity'])
                elif 'rec_count' in row:
                    rec['rec_count'] = int(row['rec_count'])
                recommendations.append(rec)
            
            # Load extended profile if it exists
            extended_profile = {}
            profile_path = f'data/profiles/user_{user_id}.json'
            if os.path.exists(profile_path):
                try:
                    with open(profile_path, 'r') as f:
                        extended_profile = json.load(f)
                    
                    # Boost content that matches user's themes
                    if 'themes' in extended_profile and extended_profile['themes']:
                        # Get content descriptions or use title + genre as proxy
                        for rec in recommendations:
                            theme_match_score = 0
                            # For each theme that might match the title or genre
                            for theme in extended_profile['themes']:
                                # Some basic rule-based matching
                                if theme == 'action_packed' and rec['genre'] in ['Action', 'Thriller']:
                                    theme_match_score += 0.1
                                elif theme == 'deep_themes' and rec['genre'] in ['Drama', 'Sci-Fi']:
                                    theme_match_score += 0.1
                                elif theme == 'character_driven' and rec['genre'] in ['Drama']:
                                    theme_match_score += 0.1
                                elif theme == 'dark' and rec['genre'] in ['Horror', 'Thriller', 'Crime']:
                                    theme_match_score += 0.1
                                elif theme == 'light_hearted' and rec['genre'] in ['Comedy', 'Romance']:
                                    theme_match_score += 0.1
                                # More advanced matching would use actual content analysis
                            
                            # Add theme matching boost to similarity score
                            if 'similarity' in rec:
                                rec['similarity'] = min(1.0, rec['similarity'] + theme_match_score)
                            elif 'rec_count' in rec:
                                rec['similarity'] = min(1.0, (rec['rec_count'] / 5) + theme_match_score)
                        
                    # Apply genre rating boosting if available
                    if 'genre_ratings' in extended_profile and extended_profile['genre_ratings']:
                        for rec in recommendations:
                            genre_lower = rec['genre'].lower()
                            for genre, rating in extended_profile['genre_ratings'].items():
                                if genre in genre_lower or (genre == 'scifi' and 'sci-fi' in genre_lower):
                                    # Add a boost based on the user's rating of this genre (1-5)
                                    boost = (rating - 3) * 0.05  # -0.1 to +0.1 adjustment
                                    if 'similarity' in rec:
                                        rec['similarity'] = max(0, min(1.0, rec['similarity'] + boost))
                                    # Else case already handled above
                except Exception as profile_error:
                    print(f"Error processing extended profile: {profile_error}")
            
            # Sort by similarity score
            if recommendations and 'similarity' in recommendations[0]:
                recommendations.sort(key=lambda x: x['similarity'], reverse=True)
            
            # If we have viewing time preference, customize recommendations order
            viewing_time = session.get('viewing_time')
            if viewing_time:
                if viewing_time == 'morning':
                    # Prefer lighter content in the morning
                    recs_light = [rec for rec in recommendations if rec['genre'] not in ['Drama', 'Crime', 'Thriller', 'Horror']]
                    recs_heavy = [rec for rec in recommendations if rec['genre'] in ['Drama', 'Crime', 'Thriller', 'Horror']]
                    recommendations = recs_light + recs_heavy
                elif viewing_time == 'late-night':
                    # Prefer sci-fi and thriller content at night
                    recs_night = [rec for rec in recommendations if rec['genre'] in ['Sci-Fi', 'Thriller', 'Horror']]
                    recs_other = [rec for rec in recommendations if rec['genre'] not in ['Sci-Fi', 'Thriller', 'Horror']]
                    recommendations = recs_night + recs_other
            
            # If we have watching habit, adjust recommendations
            watch_habit = session.get('watch_habit')
            if watch_habit == 'binge':
                # Promote series for binge watchers (titles with numbers or "Season" in them)
                series_first = [rec for rec in recommendations if ' ' in rec['title'] and any(char.isdigit() for char in rec['title'])]
                others = [rec for rec in recommendations if rec not in series_first]
                recommendations = series_first + others
            
            # Limit to top 10
            recommendations = recommendations[:10]
        
        return jsonify({
            'success': True,
            'recommendations': recommendations,
            'using_quantum': using_quantum
        })
    
    except Exception as e:
        print(f"Error generating recommendations: {e}")
        # Try to return some generic recommendations in case of error
        try:
            popular_movies = recommender.movies_df.sort_values('rating', ascending=False).head(10)
            fallback_recommendations = []
            for _, row in popular_movies.iterrows():
                rec = {
                    'id': int(row['movie_id']),
                    'title': row['title'],
                    'genre': row['genre'],
                    'rating': float(row['rating'])
                }
                fallback_recommendations.append(rec)
                
            return jsonify({
                'success': True,
                'recommendations': fallback_recommendations,
                'using_quantum': False,
                'fallback': True
            })
        except:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

@main_bp.route('/movies/<int:movie_id>')
def get_movie(movie_id):
    """Get details for a specific movie."""
    try:
        movie = recommender.movies_df[recommender.movies_df['movie_id'] == movie_id]
        
        if movie.empty:
            return jsonify({
                'success': False,
                'error': f"Movie ID {movie_id} not found"
            }), 404
            
        movie_data = {
            'id': int(movie_id),
            'title': movie['title'].values[0],
            'genre': movie['genre'].values[0],
            'release_year': int(movie['release_year'].values[0]),
            'rating': float(movie['rating'].values[0]),
            'runtime': int(movie['runtime'].values[0]),
            'is_original': bool(movie['is_original'].values[0])
        }
        
        return jsonify({
            'success': True,
            'movie': movie_data
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@main_bp.route('/update_profile', methods=['GET', 'POST'])
def update_profile():
    """Update or fetch the user's profile with extended information."""
    user_id = session.get('user_id', 1)  # Default to user 1 if not in session
    
    if request.method == 'GET':
        # Return current profile
        profile_path = f'data/profiles/user_{user_id}.json'
        if os.path.exists(profile_path):
            try:
                with open(profile_path, 'r') as f:
                    profile_data = json.load(f)
                return jsonify({'success': True, 'profile': profile_data})
            except Exception as e:
                print(f"Error reading profile: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
        else:
            return jsonify({'success': True, 'profile': {}})
    
    # POST request - update profile
    profile_data = request.json
    
    if not profile_data:
        return jsonify({'success': False, 'error': 'No profile data provided'}), 400
    
    try:
        # Create profiles directory if it doesn't exist
        os.makedirs('data/profiles', exist_ok=True)
        
        # Load existing profile if it exists
        profile_path = f'data/profiles/user_{user_id}.json'
        existing_profile = {}
        
        if os.path.exists(profile_path):
            with open(profile_path, 'r') as f:
                existing_profile = json.load(f)
        
        # Update with new data
        existing_profile.update(profile_data)
        
        # Save updated profile
        with open(profile_path, 'w') as f:
            json.dump(existing_profile, f)
        
        # Update session variables for use in recommendation filtering
        if 'watch_habit' in existing_profile:
            session['watch_habit'] = existing_profile['watch_habit']
        if 'viewing_time' in existing_profile:
            session['viewing_time'] = existing_profile['viewing_time']
        if 'themes' in profile_data:
            session['themes'] = profile_data['themes']
        
        return jsonify({'success': True})
    
    except Exception as e:
        print(f"Error updating profile: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@main_bp.route('/profile')
def profile():
    """Show the user profile page."""
    return render_template('profile.html')

@main_bp.route('/about')
def about():
    """Show the about page."""
    return render_template('about.html')

@main_bp.route('/clear_session')
def clear_session():
    """Debug endpoint to clear the session."""
    prev_session = dict(session)
    session.clear()
    return f"<h1>Session cleared</h1><p>Previous session: {prev_session}</p><a href='/'>Go to home</a>" 