// Dashboard Notification System
window.DashboardNotifications = (function() {
    'use strict';

    // Initialize notification styles if not already present
    function initNotificationStyles() {
        if (!document.querySelector('#notification-keyframes')) {
            const style = document.createElement('style');
            style.id = 'notification-keyframes';
            style.textContent = `
                @keyframes slideInNotification {
                    from { transform: translateX(100%); opacity: 0; }
                    to { transform: translateX(0); opacity: 1; }
                }
                @keyframes slideOutNotification {
                    from { transform: translateX(0); opacity: 1; }
                    to { transform: translateX(100%); opacity: 0; }
                }
            `;
            document.head.appendChild(style);
        }
    }

    // Show notification function
    function showNotification(message, type = 'success', duration = 5000) {
        // Initialize styles
        initNotificationStyles();
        
        // Remove existing notification
        const existing = document.querySelector('.notification');
        if (existing) {
            existing.remove();
        }
        
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification ${type !== 'success' ? 'notification-' + type : ''}`;
        
        // Set notification content
        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas fa-${getNotificationIcon(type)} notification-icon"></i>
                <span>${message}</span>
                <button class="notification-close" onclick="this.parentElement.parentElement.remove()">&times;</button>
            </div>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Auto remove after specified duration
        setTimeout(() => {
            if (notification.parentElement) {
                notification.style.animation = 'slideOutNotification 0.3s ease forwards';
                setTimeout(() => {
                    if (notification.parentElement) {
                        notification.remove();
                    }
                }, 300);
            }
        }, duration);
        
        return notification;
    }

    // Get appropriate icon for notification type
    function getNotificationIcon(type) {
        const icons = {
            success: 'check-circle',
            error: 'exclamation-triangle',
            warning: 'exclamation-circle',
            info: 'info-circle'
        };
        return icons[type] || icons.success;
    }

    // Loading state management
    function setLoading(button, isLoading) {
        if (!button) return;
        
        const originalHTML = button.dataset.originalHTML || button.innerHTML;
        const originalText = button.dataset.originalText || button.textContent;
        
        if (!button.dataset.originalHTML) {
            button.dataset.originalHTML = originalHTML;
            button.dataset.originalText = originalText;
        }
        
        if (isLoading) {
            button.disabled = true;
            button.classList.add('btn-loading');
            
            // Check if button contains only an icon
            if (button.innerHTML.includes('<i class="fas')) {
                button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
            } else {
                button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
            }
        } else {
            button.disabled = false;
            button.classList.remove('btn-loading');
            button.innerHTML = originalHTML;
        }
    }

    // Enhanced fetch with loading state and error handling
    async function fetchWithFeedback(url, options = {}, button = null, successMessage = '') {
        if (button) setLoading(button, true);
        
        try {
            const response = await fetch(url, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            
            const data = await response.json();
            
            if (response.ok && data.success) {
                if (successMessage) {
                    showNotification(data.message || successMessage);
                }
                return data;
            } else {
                showNotification(data.error || data.message || 'Operation failed', 'error');
                throw new Error(data.error || 'Operation failed');
            }
        } catch (error) {
            if (error.name === 'TypeError' && error.message.includes('fetch')) {
                showNotification('Network error. Please check your connection and try again.', 'error');
            } else if (!error.message.includes('Operation failed')) {
                showNotification('An unexpected error occurred. Please try again.', 'error');
            }
            throw error;
        } finally {
            if (button) setLoading(button, false);
        }
    }

    // Confirmation dialog with better styling
    function showConfirmDialog(message, confirmCallback, cancelCallback = null) {
        // For now, use native confirm - can be enhanced later with custom modal
        if (confirm(message)) {
            if (typeof confirmCallback === 'function') {
                confirmCallback();
            }
        } else if (typeof cancelCallback === 'function') {
            cancelCallback();
        }
    }

    // Public API
    return {
        show: showNotification,
        success: (message, duration) => showNotification(message, 'success', duration),
        error: (message, duration) => showNotification(message, 'error', duration),
        warning: (message, duration) => showNotification(message, 'warning', duration),
        info: (message, duration) => showNotification(message, 'info', duration),
        setLoading: setLoading,
        fetchWithFeedback: fetchWithFeedback,
        confirm: showConfirmDialog
    };
})();

// Convenience aliases
window.showNotification = window.DashboardNotifications.show;
window.setLoading = window.DashboardNotifications.setLoading;