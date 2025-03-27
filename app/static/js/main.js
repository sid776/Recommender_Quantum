// DOM Elements
const recommendationsSlider = document.getElementById('recommendations-slider');
const popularSlider = document.getElementById('popular-slider');
const continueWatchingSlider = document.getElementById('continue-watching-slider');
const movieModal = document.getElementById('movie-modal');
const closeModal = document.querySelector('.close-modal');
const userInfo = document.getElementById('user-info');
const loginForm = document.getElementById('login-form');
const userSelect = document.getElementById('user-select');
const header = document.querySelector('header');

// Current user
let currentUser = 1;

// Fetch recommendations from API
async function fetchRecommendations() {
    try {
        const response = await fetch('/recommendations');
        const data = await response.json();
        
        if (data.success) {
            displayRecommendations(data.recommendations, data.using_quantum);
            populateOtherSliders(data.recommendations);
        } else {
            console.error('Error fetching recommendations:', data.error);
            recommendationsSlider.innerHTML = `<p class="error">Error loading recommendations: ${data.error}</p>`;
        }
    } catch (error) {
        console.error('Error fetching recommendations:', error);
        recommendationsSlider.innerHTML = `<p class="error">Error loading recommendations: ${error.message}</p>`;
    }
}

// Display recommendations in the slider
function displayRecommendations(recommendations, usingQuantum) {
    recommendationsSlider.innerHTML = '';
    
    recommendations.forEach(movie => {
        const movieCard = createMovieCard(movie, usingQuantum);
        recommendationsSlider.appendChild(movieCard);
    });
}

// Create a movie card element
function createMovieCard(movie, usingQuantum) {
    const movieCard = document.createElement('div');
    movieCard.className = 'movie-card';
    movieCard.dataset.id = movie.id;
    
    // Calculate match percentage (either from similarity or a default value)
    let matchPercentage = 0;
    if (movie.similarity) {
        matchPercentage = Math.round(movie.similarity * 100);
    } else if (movie.rec_count) {
        // Normalize rec_count to a percentage (assuming max count could be 5)
        matchPercentage = Math.min(Math.round((movie.rec_count / 5) * 100), 100);
    } else {
        // Default match percentage based on rating
        matchPercentage = Math.round((movie.rating / 10) * 100);
    }
    
    // Create poster with fallback gradient
    const posterColor = getRandomDarkColor();
    
    movieCard.innerHTML = `
        <div class="quantum-match">${matchPercentage}% Match</div>
        <div class="movie-poster" style="background: linear-gradient(45deg, ${posterColor}, #222);">
            <div class="movie-poster-placeholder">${movie.title.charAt(0)}</div>
            <img class="movie-poster-img" src="https://source.unsplash.com/300x450/?movie" alt="${movie.title}" loading="lazy">
        </div>
        <div class="movie-title">${movie.title}</div>
        <div class="movie-info">
            <span class="movie-genre">${movie.genre}</span>
            <span class="movie-rating"><i class="fas fa-star"></i>${movie.rating.toFixed(1)}</span>
        </div>
    `;
    
    // Add event listener for movie card click
    movieCard.addEventListener('click', () => {
        openMovieModal(movie, matchPercentage, usingQuantum);
    });
    
    return movieCard;
}

// Open the movie modal with details
function openMovieModal(movie, matchPercentage, usingQuantum) {
    // Fetch additional movie details if needed
    fetchMovieDetails(movie.id)
        .then(movieDetails => {
            // Update modal content
            document.getElementById('modal-title').textContent = movie.title;
            document.getElementById('modal-year').textContent = movieDetails.release_year || '2023';
            document.getElementById('modal-runtime').textContent = 
                `${Math.floor(movieDetails.runtime / 60)}h ${movieDetails.runtime % 60}m`;
            document.getElementById('modal-rating').textContent = movie.rating.toFixed(1);
            document.getElementById('modal-genre').textContent = movie.genre;
            document.getElementById('modal-description').textContent = 
                `This is a ${movie.genre} ${movieDetails.is_original ? 'Netflix Original' : ''} title with great reviews. ` +
                `Released in ${movieDetails.release_year}, it has captivated audiences with its unique storyline and outstanding performances.`;
            document.getElementById('modal-quantum-score').textContent = `${matchPercentage}%`;
            document.getElementById('modal-engine-type').textContent = 
                usingQuantum ? 'Quantum Circuit' : 'Classical Algorithm';
            
            // Show the modal
            movieModal.style.display = 'block';
        })
        .catch(error => {
            console.error('Error fetching movie details:', error);
            // Show modal with basic info anyway
            document.getElementById('modal-title').textContent = movie.title;
            document.getElementById('modal-genre').textContent = movie.genre;
            document.getElementById('modal-rating').textContent = movie.rating.toFixed(1);
            document.getElementById('modal-quantum-score').textContent = `${matchPercentage}%`;
            document.getElementById('modal-engine-type').textContent = 
                usingQuantum ? 'Quantum Circuit' : 'Classical Algorithm';
            
            movieModal.style.display = 'block';
        });
}

// Fetch additional movie details
async function fetchMovieDetails(movieId) {
    try {
        const response = await fetch(`/movies/${movieId}`);
        const data = await response.json();
        
        if (data.success) {
            return data.movie;
        } else {
            throw new Error(data.error || 'Failed to load movie details');
        }
    } catch (error) {
        console.error('Error fetching movie details:', error);
        throw error;
    }
}

// Populate other sliders with sample data based on recommendations
function populateOtherSliders(recommendations) {
    // For popular slider, use the first 8 recommendations but in different order
    const popularMovies = [...recommendations]
        .sort((a, b) => b.rating - a.rating)
        .slice(0, 8);
    
    popularSlider.innerHTML = '';
    popularMovies.forEach(movie => {
        const movieCard = createMovieCard(movie, false);
        popularSlider.appendChild(movieCard);
    });
    
    // For continue watching, use first 5 recommendations with progress bars
    const continueWatchingMovies = recommendations.slice(0, 5);
    
    continueWatchingSlider.innerHTML = '';
    continueWatchingMovies.forEach(movie => {
        const movieCard = createMovieCard(movie, false);
        
        // Add progress bar
        const progressBar = document.createElement('div');
        progressBar.className = 'progress-bar';
        
        const progress = document.createElement('div');
        progress.className = 'progress';
        progress.style.width = `${Math.floor(Math.random() * 90) + 10}%`;
        
        progressBar.appendChild(progress);
        movieCard.appendChild(progressBar);
        
        continueWatchingSlider.appendChild(movieCard);
    });
}

// Close the movie modal
function closeMovieModal() {
    movieModal.style.display = 'none';
}

// Login user
async function loginUser(userId) {
    try {
        const response = await fetch('/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ user_id: userId })
        });
        
        const data = await response.json();
        
        if (data.success) {
            currentUser = userId;
            userInfo.textContent = `User ${userId}`;
            fetchRecommendations();
        } else {
            console.error('Login failed:', data.error);
        }
    } catch (error) {
        console.error('Login error:', error);
    }
}

// Helper function to generate random dark colors for movie card backgrounds
function getRandomDarkColor() {
    const h = Math.floor(Math.random() * 360);
    const s = Math.floor(Math.random() * 30) + 70;
    const l = Math.floor(Math.random() * 15) + 15;
    return `hsl(${h}, ${s}%, ${l}%)`;
}

// Add scroll event listener for header
window.addEventListener('scroll', () => {
    if (window.scrollY > 50) {
        header.classList.add('scrolled');
    } else {
        header.classList.remove('scrolled');
    }
});

// Event listeners
document.addEventListener('DOMContentLoaded', () => {
    // Fetch initial recommendations
    fetchRecommendations();
    
    // Close modal when X is clicked
    closeModal.addEventListener('click', closeMovieModal);
    
    // Close modal when clicking outside
    window.addEventListener('click', (event) => {
        if (event.target === movieModal) {
            closeMovieModal();
        }
    });
    
    // Login form submission
    loginForm.addEventListener('submit', (event) => {
        event.preventDefault();
        const userId = userSelect.value;
        loginUser(userId);
    });
});

// Add escape key listener to close modal
document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && movieModal.style.display === 'block') {
        closeMovieModal();
    }
}); 