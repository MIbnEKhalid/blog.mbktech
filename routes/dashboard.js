import express from 'express';
import { pool } from './pool.js';
import { marked } from 'marked';
import Prism from 'prismjs';
import multer from 'multer';
import crypto from 'crypto';
import path from 'path';
import rateLimit from 'express-rate-limit';
import { uploadFile, downloadFile, deleteFile, deleteFiles, listfiles, getFileMetadata, fileExists, checkR2Health, generateSignedUrl } from './pool.js';

// File signature validation for images
const IMAGE_SIGNATURES = {
    'image/jpeg': [[0xFF, 0xD8, 0xFF]],
    'image/png': [[0x89, 0x50, 0x4E, 0x47]],
    'image/gif': [[0x47, 0x49, 0x46, 0x38, 0x37, 0x61], [0x47, 0x49, 0x46, 0x38, 0x39, 0x61]],
    'image/webp': [[0x52, 0x49, 0x46, 0x46]],
    'image/svg+xml': [[0x3C, 0x3F, 0x78, 0x6D], [0x3C, 0x73, 0x76, 0x67]] // <?xml or <svg
};

// Validate file signature against declared MIME type
function validateFileSignature(buffer, mimeType) {
    const signatures = IMAGE_SIGNATURES[mimeType];
    if (!signatures) return false;
    
    return signatures.some(signature => {
        return signature.every((byte, index) => buffer[index] === byte);
    });
}

// Generate secure filename
function generateSecureFilename(originalname) {
    const ext = path.extname(originalname).toLowerCase();
    const allowedExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg'];
    
    if (!allowedExtensions.includes(ext)) {
        throw new Error('Invalid file extension');
    }
    
    const randomName = crypto.randomUUID();
    return `blog-images/${randomName}${ext}`;
}

// Upload rate limiter - stricter for file uploads
const uploadRateLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 5, // 5 uploads per IP per window
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many upload requests. Please try again later.' },
    handler: (req, res) => {
        res.status(429).json({ error: 'Too many upload requests. Please try again later.' });
    }
});

// Configure multer with disk storage and enhanced validation
const upload = multer({ 
    storage: multer.diskStorage({
        destination: (req, file, cb) => {
            cb(null, '/tmp'); // Use temp directory
        },
        filename: (req, file, cb) => {
            try {
                const filename = generateSecureFilename(file.originalname);
                cb(null, path.basename(filename));
            } catch (error) {
                cb(error);
            }
        }
    }),
    limits: {
        fileSize: 20 * 1024 * 1024, // 20MB limit (consistent with client)
        files: 1 // Only one file per request
    },
    fileFilter: (req, file, cb) => {
        // Validate MIME type
        const allowedMimes = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/svg+xml'];
        if (!allowedMimes.includes(file.mimetype)) {
            return cb(new Error('Invalid file type. Only JPEG, PNG, GIF, WebP, and SVG images are allowed.'), false);
        }
        
        // Validate filename
        if (!file.originalname || file.originalname.length > 255) {
            return cb(new Error('Invalid filename'), false);
        }
        
        cb(null, true);
    }
});

// Configure marked with syntax highlighting
marked.setOptions({
    highlight: function (code, lang) {
        if (Prism.languages[lang]) {
            return Prism.highlight(code, Prism.languages[lang], lang);
        }
        return code;
    },
    breaks: true,
    gfm: true
});

const router = express.Router();

// Dashboard home
router.get('/', async (req, res) => {
    try {
        const [
            postStats,
            commentStats,
            recentPosts,
            recentComments
        ] = await Promise.all([
            // Post statistics
            pool.query(`
                SELECT 
                    COUNT(*) as total_posts,
                    COUNT(CASE WHEN status = 'published' THEN 1 END) as published_posts,
                    COUNT(CASE WHEN status = 'draft' THEN 1 END) as draft_posts,
                    COUNT(CASE WHEN status = 'private' THEN 1 END) as private_posts
                FROM Posts
            `),
            // Comment statistics
            pool.query(`
                SELECT 
                    COUNT(*) as total_comments,
                    COUNT(CASE WHEN is_approved = true THEN 1 END) as approved_comments,
                    COUNT(CASE WHEN is_approved = false THEN 1 END) as pending_comments
                FROM Comments
            `),
            // Recent posts
            pool.query(`
                SELECT p.*, STRING_AGG(c.name, ', ') as categories
                FROM Posts p
                LEFT JOIN Post_Categories pc ON p.id = pc.post_id
                LEFT JOIN Categories c ON pc.category_id = c.id
                GROUP BY p.id
                ORDER BY p.created_at DESC
                LIMIT 5
            `),
            // Recent comments
            pool.query(`
                SELECT c.*, p.title as post_title, p.slug as post_slug
                FROM Comments c
                LEFT JOIN Posts p ON c.post_id = p.id
                ORDER BY c.created_at DESC
                LIMIT 5
            `)
        ]);

        res.render('dashboard/index.handlebars', {
            layout: 'dashboard',
            active: 'dashboard',
            postStats: postStats.rows[0],
            commentStats: commentStats.rows[0],
            recentPosts: recentPosts.rows,
            recentComments: recentComments.rows
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error loading dashboard', code: 500 });
    }
});

// Dashboard home - Posts management
router.get('/posts', async (req, res) => {
    try {
        const [posts, stats, categories] = await Promise.all([
            pool.query(
                `SELECT p.*, p."UserName" as author_name, STRING_AGG(DISTINCT c.name, ', ') as categories 
                FROM Posts p 
                LEFT JOIN Post_Categories pc ON p.id = pc.post_id 
                LEFT JOIN Categories c ON pc.category_id = c.id 
                GROUP BY p.id 
                ORDER BY p.created_at DESC`
            ),
            pool.query(`
                SELECT 
                    COUNT(*) as total_posts,
                    COUNT(CASE WHEN status = 'published' THEN 1 END) as published_posts,
                    COUNT(CASE WHEN status = 'draft' THEN 1 END) as draft_posts,
                    COUNT(CASE WHEN status = 'private' THEN 1 END) as private_posts,
                    (SELECT COUNT(*) FROM Categories) as total_categories
                FROM Posts
            `),
            pool.query('SELECT * FROM Categories ORDER BY name')
        ]);

        res.render('dashboard/posts.handlebars', {
            layout: 'dashboard',
            active: 'posts',
            posts: posts.rows,
            categories: categories.rows,
            totalPosts: stats.rows[0].total_posts,
            publishedPosts: stats.rows[0].published_posts,
            draftPosts: stats.rows[0].draft_posts,
            privatePosts: stats.rows[0].private_posts,
            totalCategories: stats.rows[0].total_categories
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error fetching posts', code: 500 });
    }
});

// Create new post form
router.get('/posts/create', async (req, res) => {
    try {
        const [categories, availableTags] = await Promise.all([
            pool.query('SELECT * FROM Categories ORDER BY name'),
            pool.query('SELECT * FROM Tags ORDER BY name')
        ]);
        res.render('dashboard/edit-post.handlebars', {
            layout: 'dashboard',
            active: 'posts',
            categories: categories.rows,
            availableTags: availableTags.rows
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error loading create post form', code: 500 });
    }
});

// Edit post form
router.get('/posts/edit/:id', async (req, res) => {
    try {
        const [post, categories, postCategories, postTags, availableTags] = await Promise.all([
            pool.query('SELECT * FROM Posts WHERE id = $1', [req.params.id]),
            pool.query('SELECT * FROM Categories ORDER BY name'),
            pool.query(`
                SELECT c.id
                FROM Categories c
                JOIN Post_Categories pc ON c.id = pc.category_id
                WHERE pc.post_id = $1
            `, [req.params.id]),
            pool.query(`
                SELECT t.name
                FROM Tags t
                JOIN Post_Tags pt ON t.id = pt.tag_id
                WHERE pt.post_id = $1
            `, [req.params.id]),
            pool.query('SELECT * FROM Tags ORDER BY name')
        ]);

        if (post.rows.length === 0) {
            return res.status(404).render('error.handlebars', { message: 'Post not found', code: 404 });
        }

        // Add tags and categories to post object
        post.rows[0].tags = postTags.rows.map(tag => tag.name);
        post.rows[0].categoryIds = postCategories.rows.map(cat => cat.id);

        res.render('dashboard/edit-post.handlebars', {
            layout: 'dashboard',
            active: 'posts',
            post: post.rows[0],
            categories: categories.rows,
            availableTags: availableTags.rows,
            isPublished: post.rows[0].status === 'published',
            isPrivate: post.rows[0].status === 'private'
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error loading edit post form', code: 500 });
    }
});

// Comments management
router.get('/comments', async (req, res) => {
    try {
        const [comments, stats] = await Promise.all([
            pool.query(`
                SELECT c.*, p.title as post_title, p.slug as post_slug 
                FROM comments c 
                LEFT JOIN posts p ON c.post_id = p.id 
                ORDER BY c.created_at DESC
            `),
            pool.query(`
                SELECT 
                    COUNT(*) as total_comments,
                    COUNT(CASE WHEN is_approved = true THEN 1 END) as approved_comments,
                    COUNT(CASE WHEN is_approved = false THEN 1 END) as pending_comments
                FROM comments
            `)
        ]);

        res.render('dashboard/comments.handlebars', {
            layout: 'dashboard',
            active: 'comments',
            comments: comments.rows,
            totalComments: stats.rows[0].total_comments,
            approvedComments: stats.rows[0].approved_comments,
            pendingComments: stats.rows[0].pending_comments
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error fetching comments', code: 500 });
    }
});

// Categories management
router.get('/categories', async (req, res) => {
    try {
        const [result, stats] = await Promise.all([
            pool.query(`
                SELECT c.*, COUNT(DISTINCT p.id) as post_count 
                FROM Categories c 
                LEFT JOIN Post_Categories pc ON c.id = pc.category_id
                LEFT JOIN Posts p ON pc.post_id = p.id
                GROUP BY c.id 
                ORDER BY c.name
            `),
            pool.query(`
                SELECT 
                    COUNT(*) as total_categories,
                    (SELECT COUNT(*) FROM posts) as total_posts
                FROM categories
            `)
        ]);

        res.render('dashboard/categories.handlebars', {
            layout: 'dashboard',
            active: 'categories',
            categories: result.rows,
            totalCategories: stats.rows[0].total_categories,
            totalPosts: stats.rows[0].total_posts
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error fetching categories', code: 500 });
    }
});

// Tags management
router.get('/tags', async (req, res) => {
    try {
        const [tags, stats] = await Promise.all([
            pool.query(`
                SELECT t.*, COUNT(pt.post_id) as post_count 
                FROM Tags t 
                LEFT JOIN Post_Tags pt ON t.id = pt.tag_id 
                GROUP BY t.id 
                ORDER BY t.name
            `),
            pool.query(`
                SELECT 
                    COUNT(*) as total_tags,
                    COUNT(DISTINCT pt.post_id) as posts_with_tags,
                    (SELECT COUNT(*) FROM posts) as total_posts
                FROM tags t
                LEFT JOIN post_tags pt ON t.id = pt.tag_id
            `)
        ]);

        res.render('dashboard/tags.handlebars', {
            layout: 'dashboard',
            active: 'tags',
            tags: tags.rows,
            totalTags: stats.rows[0].total_tags,
            postsWithTags: stats.rows[0].posts_with_tags,
            totalPosts: stats.rows[0].total_posts
        });
    } catch (err) {
        console.error(err);
        res.status(500).render('error.handlebars', { message: 'Error fetching tags', code: 500 });
    }
});

// API endpoints for managing content

// Posts API endpoints
router.post('/api/posts', async (req, res) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const { title, content, excerpt, categories, tags, status, preview_image } = req.body;

        // Validate required fields
        if (!title || !content) {
            await client.query('ROLLBACK');
            return res.status(400).json({ message: 'Title and content are required' });
        }

        // Validate categories
        let categoryIds = [];
        if (categories) {
            try {
                if (typeof categories === 'string') {
                    categoryIds = JSON.parse(categories);
                } else if (Array.isArray(categories)) {
                    categoryIds = categories;
                }
            } catch (e) {
                await client.query('ROLLBACK');
                return res.status(400).json({ message: 'Invalid categories format' });
            }
        }

        if (!categoryIds || categoryIds.length === 0) {
            await client.query('ROLLBACK');
            return res.status(400).json({ message: 'At least one category is required' });
        }

        const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

        // Insert post
        const postResult = await client.query(
            'INSERT INTO Posts (title, slug, excerpt, content_markdown, status, preview_image, "UserName") VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id',
            [title, slug, excerpt || null, content, status || 'draft', preview_image, req.session.user.username]
        );

        // Add categories
        for (const categoryId of categoryIds) {
            if (typeof categoryId === 'number' && !isNaN(categoryId)) {
                await client.query(
                    'INSERT INTO Post_Categories (post_id, category_id) VALUES ($1, $2)',
                    [postResult.rows[0].id, categoryId]
                );
            }
        }

        // Handle tags
        if (tags) {
            const tagArray = JSON.parse(tags);
            for (const tagName of tagArray) {
                const normalizedTagName = tagName.toLowerCase().trim();
                // Get or create tag
                let tagResult = await client.query(
                    'SELECT id FROM Tags WHERE name = $1',
                    [normalizedTagName]
                );

                if (tagResult.rows.length === 0) {
                    tagResult = await client.query(
                        'INSERT INTO Tags (name) VALUES ($1) RETURNING id',
                        [normalizedTagName]
                    );
                }

                // Link tag to post
                await client.query(
                    'INSERT INTO Post_Tags (post_id, tag_id) VALUES ($1, $2)',
                    [postResult.rows[0].id, tagResult.rows[0].id]
                );
            }
        }

        await client.query('COMMIT');
        res.json({ success: true, id: postResult.rows[0].id });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ success: false, error: 'Failed to create post' });
    } finally {
        client.release();
    }
});

// Update post
router.put('/api/posts/:id', async (req, res) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const { title, content, excerpt, categories, tags, status, preview_image } = req.body;

        // Validate required fields
        if (!title || !content) {
            await client.query('ROLLBACK');
            return res.status(400).json({ message: 'Title and content are required' });
        }

        // Validate categories
        let categoryIds = [];
        if (categories) {
            try {
                if (typeof categories === 'string') {
                    categoryIds = JSON.parse(categories);
                } else if (Array.isArray(categories)) {
                    categoryIds = categories;
                }
            } catch (e) {
                await client.query('ROLLBACK');
                return res.status(400).json({ message: 'Invalid categories format' });
            }
        }

        if (!categoryIds || categoryIds.length === 0) {
            await client.query('ROLLBACK');
            return res.status(400).json({ message: 'At least one category is required' });
        }

        const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

        // Update post
        await client.query(
            'UPDATE Posts SET title = $1, slug = $2, excerpt = $3, content_markdown = $4, status = $5, preview_image = $6, updated_at = NOW() WHERE id = $7',
            [title, slug, excerpt || null, content, status || 'draft', preview_image, req.params.id]
        );

        // Remove existing categories
        await client.query('DELETE FROM Post_Categories WHERE post_id = $1', [req.params.id]);

        // Add new categories
        for (const categoryId of categoryIds) {
            if (typeof categoryId === 'number' && !isNaN(categoryId)) {
                await client.query(
                    'INSERT INTO Post_Categories (post_id, category_id) VALUES ($1, $2)',
                    [req.params.id, categoryId]
                );
            } else {
                console.warn(`Skipping invalid category ID: ${categoryId}`);
            }
        }

        // Remove existing tags
        await client.query('DELETE FROM Post_Tags WHERE post_id = $1', [req.params.id]);

        // Add new tags
        if (tags) {
            const tagArray = JSON.parse(tags);
            for (const tagName of tagArray) {
                const normalizedTagName = tagName.toLowerCase().trim();
                // Get or create tag
                let tagResult = await client.query(
                    'SELECT id FROM Tags WHERE name = $1',
                    [normalizedTagName]
                );

                if (tagResult.rows.length === 0) {
                    tagResult = await client.query(
                        'INSERT INTO Tags (name) VALUES ($1) RETURNING id',
                        [normalizedTagName]
                    );
                }

                // Link tag to post
                await client.query(
                    'INSERT INTO Post_Tags (post_id, tag_id) VALUES ($1, $2)',
                    [req.params.id, tagResult.rows[0].id]
                );
            }
        }

        await client.query('COMMIT');
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ success: false, error: 'Failed to update post' });
    } finally {
        client.release();
    }
});

// Delete post
router.delete('/api/posts/:id', async (req, res) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        
        // Check if post exists
        const postCheck = await client.query('SELECT id FROM posts WHERE id = $1', [req.params.id]);
        if (postCheck.rows.length === 0) {
            await client.query('ROLLBACK');
            return res.status(404).json({ success: false, error: 'Post not found' });
        }

        // Remove post comments
        await client.query('DELETE FROM comments WHERE post_id = $1', [req.params.id]);
        // Remove post tags
        await client.query('DELETE FROM post_tags WHERE post_id = $1', [req.params.id]);
        // Remove post categories
        await client.query('DELETE FROM post_categories WHERE post_id = $1', [req.params.id]);
        // Remove post
        await client.query('DELETE FROM posts WHERE id = $1', [req.params.id]);
        
        await client.query('COMMIT');
        res.json({ success: true, message: 'Post deleted successfully' });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Error deleting post:', err);
        res.status(500).json({ success: false, error: 'Failed to delete post' });
    } finally {
        client.release();
    }
});

// Comments API endpoints
router.put('/api/comments/:id/:action', upload.none(), async (req, res) => {
    const { id, action } = req.params;
    try {
        // Validate action
        if (!['approve', 'unapprove'].includes(action)) {
            return res.status(400).json({ success: false, error: 'Invalid action' });
        }

        const result = await pool.query(
            'UPDATE comments SET is_approved = $1 WHERE id = $2',
            [action === 'approve', id]
        );

        if (result.rowCount === 0) {
            return res.status(404).json({ success: false, error: 'Comment not found' });
        }

        res.json({ success: true, message: `Comment ${action}d successfully` });
    } catch (err) {
        console.error('Error moderating comment:', err);
        res.status(500).json({ success: false, error: 'Failed to moderate comment' });
    }
});

// Delete comment
router.delete('/api/comments/:id', async (req, res) => {
    try {
        const result = await pool.query('DELETE FROM comments WHERE id = $1', [req.params.id]);
        
        if (result.rowCount === 0) {
            return res.status(404).json({ success: false, error: 'Comment not found' });
        }

        res.json({ success: true, message: 'Comment deleted successfully' });
    } catch (err) {
        console.error('Error deleting comment:', err);
        res.status(500).json({ success: false, error: 'Failed to delete comment' });
    }
});

// Categories API endpoints
router.post('/api/categories', upload.none(), async (req, res) => {
    try {
        const { name, description } = req.body;
        
        if (!name || !name.trim()) {
            return res.status(400).json({ success: false, error: 'Category name is required' });
        }

        // Check if category already exists
        const existing = await pool.query(
            'SELECT id FROM categories WHERE LOWER(name) = LOWER($1)',
            [name.trim()]
        );

        if (existing.rows.length > 0) {
            return res.status(400).json({ success: false, error: 'Category with this name already exists' });
        }

        await pool.query(
            'INSERT INTO categories (name, description) VALUES ($1, $2)',
            [name.trim(), description?.trim() || null]
        );
        
        res.json({ success: true, message: 'Category created successfully' });
    } catch (err) {
        console.error('Error creating category:', err);
        if (err.code === '23505') { // Unique violation
            res.status(400).json({ success: false, error: 'Category with this name already exists' });
        } else {
            res.status(500).json({ success: false, error: 'Failed to create category' });
        }
    }
});

// Update category
router.put('/api/categories/:id', upload.none(), async (req, res) => {
    try {
        const { name, description } = req.body;
        
        if (!name || !name.trim()) {
            return res.status(400).json({ success: false, error: 'Category name is required' });
        }

        // Check if another category with this name exists
        const existing = await pool.query(
            'SELECT id FROM categories WHERE LOWER(name) = LOWER($1) AND id != $2',
            [name.trim(), req.params.id]
        );

        if (existing.rows.length > 0) {
            return res.status(400).json({ success: false, error: 'Category with this name already exists' });
        }

        const result = await pool.query(
            'UPDATE categories SET name = $1, description = $2 WHERE id = $3',
            [name.trim(), description?.trim() || null, req.params.id]
        );

        if (result.rowCount === 0) {
            return res.status(404).json({ success: false, error: 'Category not found' });
        }

        res.json({ success: true, message: 'Category updated successfully' });
    } catch (err) {
        console.error('Error updating category:', err);
        if (err.code === '23505') { // Unique violation
            res.status(400).json({ success: false, error: 'Category with this name already exists' });
        } else {
            res.status(500).json({ success: false, error: 'Failed to update category' });
        }
    }
});

// Delete category
router.delete('/api/categories/:id', async (req, res) => {
    const { id } = req.params;
    try {
        // Check if any posts are using this category
        const postCount = await pool.query('SELECT COUNT(*) FROM Post_Categories WHERE category_id = $1', [id]);
        if (parseInt(postCount.rows[0].count) > 0) {
            return res.status(400).json({ 
                success: false, 
                error: `Cannot delete category. It is used by ${postCount.rows[0].count} post(s). Please remove the category from all posts first.`
            });
        }

        const result = await pool.query('DELETE FROM categories WHERE id = $1', [id]);
        
        if (result.rowCount === 0) {
            return res.status(404).json({ success: false, error: 'Category not found' });
        }

        res.json({ success: true, message: 'Category deleted successfully' });
    } catch (err) {
        console.error('Error deleting category:', err);
        res.status(500).json({ success: false, error: 'Failed to delete category' });
    }
});

// Markdown preview endpoint
router.post('/api/markdown-preview', upload.none(), (req, res) => {
    try {
        const { markdown } = req.body;
        if (!markdown) {
            return res.status(400).json({ error: 'Markdown content is required' });
        }

        const html = marked(markdown);
        res.json({ html });
    } catch (err) {
        console.error('Error converting markdown:', err);
        res.status(500).json({ error: 'Failed to convert markdown' });
    }
});

// Tags API endpoints
router.post('/api/tags', upload.none(), async (req, res) => {
    try {
        const { name } = req.body;
        
        if (!name || !name.trim()) {
            return res.status(400).json({ success: false, error: 'Tag name is required' });
        }

        const normalizedTagName = name.toLowerCase().trim();

        // Check if tag already exists
        const existing = await pool.query('SELECT id FROM tags WHERE name = $1', [normalizedTagName]);
        if (existing.rows.length > 0) {
            return res.status(400).json({ success: false, error: 'Tag already exists' });
        }

        await pool.query('INSERT INTO tags (name) VALUES ($1)', [normalizedTagName]);
        res.json({ success: true, message: 'Tag created successfully' });
    } catch (err) {
        console.error('Error creating tag:', err);
        if (err.code === '23505') { // Unique violation
            res.status(400).json({ success: false, error: 'Tag already exists' });
        } else {
            res.status(500).json({ success: false, error: 'Failed to create tag' });
        }
    }
});

// Update tag
router.put('/api/tags/:id', upload.none(), async (req, res) => {
    try {
        const { name } = req.body;
        
        if (!name || !name.trim()) {
            return res.status(400).json({ success: false, error: 'Tag name is required' });
        }

        const normalizedTagName = name.toLowerCase().trim();

        // Check if another tag with this name exists
        const existing = await pool.query('SELECT id FROM tags WHERE name = $1 AND id != $2', [normalizedTagName, req.params.id]);
        if (existing.rows.length > 0) {
            return res.status(400).json({ success: false, error: 'Tag already exists' });
        }

        const result = await pool.query('UPDATE tags SET name = $1 WHERE id = $2', [normalizedTagName, req.params.id]);
        
        if (result.rowCount === 0) {
            return res.status(404).json({ success: false, error: 'Tag not found' });
        }

        res.json({ success: true, message: 'Tag updated successfully' });
    } catch (err) {
        console.error('Error updating tag:', err);
        if (err.code === '23505') { // Unique violation
            res.status(400).json({ success: false, error: 'Tag already exists' });
        } else {
            res.status(500).json({ success: false, error: 'Failed to update tag' });
        }
    }
});

// Delete tag
router.delete('/api/tags/:id', async (req, res) => {
    try {
        // Check if any posts are using this tag
        const postCount = await pool.query('SELECT COUNT(*) FROM Post_Tags WHERE tag_id = $1', [req.params.id]);
        const count = parseInt(postCount.rows[0].count);
        
        if (count > 0) {
            // Remove tag from all posts before deletion
            await pool.query('DELETE FROM Post_Tags WHERE tag_id = $1', [req.params.id]);
        }

        const result = await pool.query('DELETE FROM tags WHERE id = $1', [req.params.id]);
        
        if (result.rowCount === 0) {
            return res.status(404).json({ success: false, error: 'Tag not found' });
        }

        const message = count > 0 
            ? `Tag deleted successfully and removed from ${count} post(s)`
            : 'Tag deleted successfully';

        res.json({ success: true, message });
    } catch (err) {
        console.error('Error deleting tag:', err);
        res.status(500).json({ success: false, error: 'Failed to delete tag' });
    }
});

// Download all data
router.get('/api/download-all-data', async (req, res) => {
    try {
        const [
            posts,
            categories,
            tags,
            comments,
            postCategories,
            postTags
        ] = await Promise.all([
            pool.query('SELECT * FROM Posts'),
            pool.query('SELECT * FROM Categories'),
            pool.query('SELECT * FROM Tags'),
            pool.query('SELECT * FROM Comments'),
            pool.query('SELECT * FROM Post_Categories'),
            pool.query('SELECT * FROM Post_Tags')
        ]);

        const blogData = {
            posts: posts.rows,
            categories: categories.rows,
            tags: tags.rows,
            comments: comments.rows,
            postCategories: postCategories.rows,
            postTags: postTags.rows
        };

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', 'attachment; filename="blog_data.json"');
        res.send(JSON.stringify(blogData, null, 2));

    } catch (err) {
        console.error(err);
        res.status(500).json({ success: false, error: 'Failed to download blog data' });
    }
});

// Image upload endpoint with enhanced security
router.post('/api/upload-image', uploadRateLimiter, upload.single('image'), async (req, res) => {
    const fs = await import('fs');
    let tempFilePath = null;
    
    try {
        // Authentication check
        if (!req.session?.user?.username) {
            return res.status(401).json({ error: 'Authentication required' });
        }
        
        // Role-based authorization
        if (req.session.user.role !== 'SuperAdmin') {
            return res.status(403).json({ error: 'Insufficient permissions' });
        }
        
        if (!req.file) {
            return res.status(400).json({ error: 'No image file provided' });
        }

        const file = req.file;
        tempFilePath = file.path;
        
        // Read file buffer for signature validation
        const fileBuffer = await fs.default.promises.readFile(tempFilePath);
        
        // Validate file signature against declared MIME type
        if (!validateFileSignature(fileBuffer, file.mimetype)) {
            await fs.default.promises.unlink(tempFilePath);
            return res.status(400).json({ error: 'File signature does not match declared type' });
        }
        
        // Additional security checks
        if (fileBuffer.length !== file.size) {
            await fs.default.promises.unlink(tempFilePath);
            return res.status(400).json({ error: 'File size mismatch' });
        }
        
        // Generate secure filename
        const secureFileName = generateSecureFilename(file.originalname);
        
        // Upload to R2 with enhanced metadata
        const uploadResult = await uploadFile(
            secureFileName,
            fileBuffer,
            file.mimetype,
            {
                metadata: {
                    'original-name': file.originalname,
                    'uploaded-by': req.session.user.username,
                    'upload-type': 'blog-preview',
                    'file-hash': crypto.createHash('sha256').update(fileBuffer).digest('hex'),
                    'upload-timestamp': new Date().toISOString()
                }
            }
        );
        
        // Clean up temp file
        await fs.default.promises.unlink(tempFilePath);

        // Generate public URL using our local image serving route
        const publicUrl = `/images/${secureFileName}`;
        
        // Log successful upload for audit
        console.log(`Image uploaded successfully: ${secureFileName} by ${req.session.user.username}`);
        
        res.json({
            success: true,
            url: publicUrl,
            key: secureFileName,
            size: file.size,
            type: file.mimetype
        });
        
    } catch (err) {
        // Clean up temp file on error
        if (tempFilePath) {
            try {
                const fs = await import('fs');
                await fs.default.promises.unlink(tempFilePath);
            } catch (cleanupErr) {
                console.error('Error cleaning up temp file:', cleanupErr);
            }
        }
        
        console.error('Image upload error:', {
            error: err.message,
            user: req.session?.user?.username,
            timestamp: new Date().toISOString()
        });
        
        // Return generic error message
        res.status(500).json({ error: 'Failed to upload image' });
    }
});

// List images specifically for image browser
router.post('/api/r2/list-images', uploadRateLimiter, upload.none(), async (req, res) => {
    // Authentication check
    if (!req.session?.user?.username || req.session.user.role !== 'SuperAdmin') {
        return res.status(401).json({ error: 'Authentication required' });
    }
    try {
        const { prefix = '', searchTerm = '', maxKeys = 20, continuationToken } = req.body;
        
        // List files with the specified prefix
        const filesResult = await listfiles(prefix, {
            maxKeys: parseInt(maxKeys),
            continuationToken: continuationToken || undefined
        });

        // Filter for image files only
        const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg'];
        let images = (filesResult.Contents || []).filter(file => {
            if (!file.Key) return false;
            
            const isImage = imageExtensions.some(ext => 
                file.Key.toLowerCase().endsWith(ext)
            );
            
            // Apply search filter if provided
            if (searchTerm && !file.Key.toLowerCase().includes(searchTerm.toLowerCase())) {
                return false;
            }
            
            return isImage;
        });

        // Transform the data for frontend
        images = images.map(file => ({
            key: file.Key,
            size: file.Size,
            lastModified: file.LastModified,
            url: `/images/${file.Key}`
        }));

        res.json({
            success: true,
            images: images,
            hasMore: filesResult.IsTruncated || false,
            nextToken: filesResult.NextContinuationToken || null,
            total: images.length
        });

    } catch (err) {
        console.error('List images error:', err);
        res.status(500).json({ error: 'Failed to list images' });
    }
});

export default router;
