// ----------------------
// Register Form Handling
// ----------------------
const registerForm = document.getElementById("registerForm");
if (registerForm) {
    registerForm.onsubmit = async function (e) {
        e.preventDefault();
        const form = e.target;

        // Get the CSRF token from the template or a cookie
        const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]').value;

        const response = await fetch("/api/register/", {  // Ensure the URL matches
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-CSRFToken": csrfToken  // Add CSRF token for protection
            },
            body: JSON.stringify({
                username: form.username.value,
                email: form.email.value,
                password: form.password.value
            })
        });

        const messageEl = document.getElementById("message");

        if (response.ok) {
            messageEl.innerText = "Registered successfully!";
            messageEl.style.color = "green";
        } else {
            const errorData = await response.json();
            messageEl.innerText = errorData.detail || "Registration failed.";
            messageEl.style.color = "red";
        }
    };
}

// ----------------------
// Login Form Handling
// ----------------------
const loginForm = document.getElementById("loginForm");
if (loginForm) {
    loginForm.onsubmit = async function (e) {
        e.preventDefault();
        const username = document.getElementById("username").value;
        const password = document.getElementById("password").value;
        const messageEl = document.getElementById("message");

        if (!username || !password) {
            messageEl.innerText = "Please fill in all login fields.";
            messageEl.style.color = "red";
            return;
        }

        try {
            const response = await fetch("/api/token/", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({ username, password })
            });

            if (response.ok) {
                const data = await response.json();
                localStorage.setItem("access", data.access);
                localStorage.setItem("refresh", data.refresh);
                messageEl.innerText = "Login successful!";
                messageEl.style.color = "green";
                window.location.href = "/dashboard/";
            } else {
                messageEl.innerText = "Login failed. Check credentials.";
                messageEl.style.color = "red";
            }
        } catch (error) {
            messageEl.innerText = "An error occurred during login.";
            messageEl.style.color = "red";
        }
    };
}

// ----------------------
// Upload Template Logic
// ----------------------
function uploadTemplate() {
    const template = document.getElementById("template").value;
    const uploadBtn = document.getElementById("uploadBtn");
    const statusMessage = document.getElementById("statusMessage");



    statusMessage.textContent = 'Data transformation in process...'
    statusMessage.style.color = "green";
    statusMessage.style.display = "block";
    uploadBtn.disabled = true;
    uploadBtn.textContent = "Processing";

    fetch("/upload/", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({
            template: template,
        })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error("Server error: " + response.status);
        }
        return response.json();
    })
    .then(data => {
        if (data.filename) {
            statusMessage.textContent = "Upload successful. Redirecting...";
            statusMessage.style.color = "green";
            
            setTimeout(() => {
                window.location.href = "/download/" + encodeURIComponent(data.filename) + "/";
            }, 1000);
        } else {
            throw new Error("Invalid response from server.");
        }
    })
    .catch(error => {
        console.error('Error:', error);
        statusMessage.textContent = "Something went wrong. Please try again.";
        statusMessage.style.color = "red";
        uploadBtn.disabled = false;
        uploadBtn.textContent = "Upload Template";
    });
}


function getCSRFToken() {
    const tokenInput = document.querySelector('[name=csrfmiddlewaretoken]');
    return tokenInput ? tokenInput.value : '';
}

// -----------------------------
// Transformation Message Logic
// -----------------------------
document.addEventListener("DOMContentLoaded", function () {
    const successHeader = document.querySelector("h2");
    if (successHeader && successHeader.textContent.includes("Transformation Completed")) {
        successHeader.style.color = "green";

        const downloadLink = document.querySelector("a[download]");
        if (downloadLink) {
            downloadLink.style.fontWeight = "bold";
        }
    }
});
