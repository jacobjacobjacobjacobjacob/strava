// JavaScript to embed the Dash app in the div with id "dash-app"
window.addEventListener('DOMContentLoaded', (event) => {
    const dashAppDiv = document.getElementById('dash-app');
    if (dashAppDiv) {
        dashAppDiv.innerHTML = '<iframe src="/dash/" style="width: 100%; height: 100%; border: none;"></iframe>';
    }
});