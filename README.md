# MBK Tech Studio Blog Platform

A modern, secure, and SEO-optimized blogging platform built with Node.js and Express. Features a comprehensive admin dashboard, advanced content management, secure image upload system, and enterprise-grade security implementations.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Node Version](https://img.shields.io/badge/node-%3E%3D16-green.svg)
![Security](https://img.shields.io/badge/security-enhanced-green.svg)

## 🌟 Key Features

- **🔍 Advanced SEO Optimization**
  - Automatic sitemap generation (XML)
  - Built-in robots.txt configuration
  - Canonical URLs support
  - Schema.org structured data markup
  - Meta tags and Open Graph support
  - Twitter Cards integration

- **📝 Content Management System**
  - Rich Markdown editor with live preview
  - Split-view editing mode
  - Categories and tags organization
  - Dynamic blog post management
  - Nested comment system with moderation
  - Draft, private, and published post states

- **🖼️ Secure Image Upload System**
  - Multi-format support (JPEG, PNG, GIF, WebP, SVG)
  - File signature validation
  - Secure filename generation with UUID
  - AWS S3/R2 cloud storage integration
  - Upload rate limiting (5 uploads/15min)
  - Real-time upload progress tracking

- **🛡️ Enterprise Security & Performance**
  - Multi-layer authentication system
  - Role-based access control (SuperAdmin)
  - Advanced rate limiting protection
  - File signature validation against MIME spoofing
  - Path traversal protection
  - Memory-safe disk storage
  - GZIP compression
  - Static asset caching with CDN support
  - Audit logging for security events

- **🎨 User Experience & Interface**
  - Responsive mobile-first design
  - Fast page loading with optimizations
  - Interactive comment system with replies
  - Advanced search and filtering
  - Bookmark functionality
  - Reading time estimation
  - Dark/light theme support

## 📋 Prerequisites

- Node.js (>= 16.x)
- PostgreSQL database (Neon or local)
- AWS S3/R2 bucket for image storage
- npm or yarn package manager

## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/Mibnekhalid/blog.mbktechstudio.git
   cd blog.mbktechstudio
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure environment variables**
   Create a `.env` file with the following variables:
   ```env
   # Database Configuration
   NEON_POSTGRES=your_postgresql_connection_string
   
   # R2/S3 Bucket Configuration (JSON format)
   R2_Bucket={"ENDPOINT":"your_r2_endpoint","ACCESS_KEY_ID":"your_access_key","SECRET_ACCESS_KEY":"your_secret_key","BUCKET_NAME":"your_bucket_name"}
   
   # Server Configuration
   PORT=3126
   NODE_ENV=production
   
   # Authentication Configuration
   mbkautheVar={"SESSION_SECRET":"your_session_secret","IS_DEPLOYED":"true"}
   ```

4. **Set up the database**
   - Create the required tables using the schema in `docs/db.sql`
   - Configure your PostgreSQL database connection

5. **Start the application**
   ```bash
   # Development mode with auto-reload
   npm start
   
   # Production mode
   NODE_ENV=production node index.js
   ```
   
   The application will be available at `http://localhost:3126`

## 🔧 Configuration

### Image Upload Settings
- **Supported formats**: JPEG, PNG, GIF, WebP, SVG
- **Maximum file size**: 10MB per upload
- **Rate limiting**: 5 uploads per 15 minutes per IP
- **Storage**: AWS S3/Cloudflare R2 compatible
- **Security**: File signature validation, secure UUID naming

### Authentication & Authorization
- **Admin access**: SuperAdmin role required for dashboard
- **Session management**: Secure cookie-based sessions
- **Rate limiting**: 150 requests/2min general, 100 requests/1min dashboard

### Performance Optimizations
- **Compression**: GZIP enabled for all responses
- **Caching**: Static assets cached for 7 days
- **CDN support**: Optimized headers for CDN integration

## 🗺️ SEO Management

### Sitemap Generation

Generate fresh XML sitemaps after content updates:
```bash
npm run generate-sitemap
```

This creates:
- `sitemap.xml` - Main index
- `sitemap-posts.xml` - Blog posts
- `sitemap-categories.xml` - Categories
- `sitemap-tags.xml` - Tags

### Implemented SEO Features

- ✅ XML Sitemaps (auto-generated)
- ✅ robots.txt configuration
- ✅ Canonical URLs
- ✅ Meta tags & Open Graph
- ✅ Schema.org markup
- ✅ Twitter Cards
- ✅ Structured data
- ✅ Static asset caching
- ✅ GZIP compression

## 🏗️ Project Structure

```
├── index.js                    # Application entry point & server config
├── routes/
│   ├── blog.js                # Public blog routes & image serving
│   ├── dashboard.js           # Admin dashboard & secure upload API
│   └── pool.js                # Database & cloud storage utilities
├── views/                     # Handlebars templates
│   ├── blog/                  # Public blog templates
│   ├── dashboard/             # Admin interface templates
│   ├── layouts/               # Base layouts (main, dashboard)
│   └── partial/               # Reusable components & SEO partials
├── public/
│   ├── Assets/                # Static assets (CSS, JS, images)
│   ├── robots.txt             # SEO crawler instructions
│   └── sitemap*.xml           # Auto-generated XML sitemaps
├── tests/                     # Jest test suites
├── docs/
│   └── db.sql                 # Database schema
├── generate-sitemap.js        # SEO sitemap generator
├── jest.config.js             # Test configuration
└── vercel.json                # Deployment configuration
```

## 🧪 Testing

Run the test suite:
```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm test -- --coverage
```

## 🔐 Security Features

### Upload Security
- ✅ File signature validation (magic number checking)
- ✅ MIME type verification
- ✅ Secure UUID-based filename generation
- ✅ Path traversal protection
- ✅ Authentication & authorization checks
- ✅ Rate limiting (5 uploads/15min)
- ✅ File size limits (10MB max)
- ✅ Temporary file cleanup

### General Security
- ✅ Role-based access control
- ✅ Session security with secure cookies
- ✅ Rate limiting on all endpoints
- ✅ CORS protection
- ✅ Content Security Policy headers
- ✅ Audit logging for admin actions

## 📊 Monitoring & Analytics

### Performance Metrics
- Request/response timing logging
- Upload success/failure tracking
- Rate limit violation monitoring
- Database connection pooling stats

### Security Auditing
- Failed authentication attempts
- Upload security violations
- Admin action logging with user context
- IP-based rate limit violations

## 🔄 Development Roadmap

### ✅ Completed
- [x] Secure image upload system with file validation
- [x] Multi-format image support (JPEG, PNG, GIF, WebP, SVG)
- [x] Enterprise-grade security implementation
- [x] Advanced rate limiting and authentication
- [x] Memory-safe file handling
- [x] Comprehensive audit logging

### 🚧 In Progress
- [ ] Enhanced mobile responsiveness optimization
- [ ] Advanced comment moderation tools
- [ ] Bulk operations in admin dashboard

### 📝 Planned Features
- [ ] Multi-language support (i18n)
- [ ] Advanced analytics dashboard
- [ ] Email notification system
- [ ] Content scheduling functionality
- [ ] API endpoints for headless CMS usage
- [ ] Plugin system architecture
- [ ] Advanced caching strategies (Redis)
- [ ] Full-text search with Elasticsearch

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Muhammad Bin Khalid**
- Website: [mbktechstudio.com](https://www.mbktechstudio.com)
- Blog: [blog.mbktech.org](https://blog.mbktech.org)
- GitHub: [@Mibnekhalid](https://github.com/Mibnekhalid)