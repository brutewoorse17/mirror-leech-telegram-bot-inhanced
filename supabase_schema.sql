-- Supabase Schema for MLTBv2 Bot Database
-- Run this script in your Supabase SQL editor to create the required tables

-- Enable Row Level Security
-- This is recommended for production, but you may need to configure policies

-- Settings table to store bot configuration
CREATE TABLE IF NOT EXISTS mltb_settings (
    id BIGSERIAL PRIMARY KEY,
    bot_id TEXT NOT NULL,
    category TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bot_id, category)
);

-- Users table to store user-specific data
CREATE TABLE IF NOT EXISTS mltb_users (
    id BIGSERIAL PRIMARY KEY,
    bot_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bot_id, user_id)
);

-- RSS table to store RSS feed configurations
CREATE TABLE IF NOT EXISTS mltb_rss (
    id BIGSERIAL PRIMARY KEY,
    bot_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bot_id, user_id)
);

-- Tasks table to store incomplete tasks
CREATE TABLE IF NOT EXISTS mltb_tasks (
    id BIGSERIAL PRIMARY KEY,
    bot_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    cid TEXT NOT NULL,
    tag TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(bot_id, task_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_mltb_settings_bot_id ON mltb_settings(bot_id);
CREATE INDEX IF NOT EXISTS idx_mltb_settings_category ON mltb_settings(category);
CREATE INDEX IF NOT EXISTS idx_mltb_users_bot_id ON mltb_users(bot_id);
CREATE INDEX IF NOT EXISTS idx_mltb_users_user_id ON mltb_users(user_id);
CREATE INDEX IF NOT EXISTS idx_mltb_rss_bot_id ON mltb_rss(bot_id);
CREATE INDEX IF NOT EXISTS idx_mltb_rss_user_id ON mltb_rss(user_id);
CREATE INDEX IF NOT EXISTS idx_mltb_tasks_bot_id ON mltb_tasks(bot_id);
CREATE INDEX IF NOT EXISTS idx_mltb_tasks_task_id ON mltb_tasks(task_id);

-- Create trigger function for updating updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for auto-updating timestamps
DROP TRIGGER IF EXISTS update_mltb_settings_updated_at ON mltb_settings;
CREATE TRIGGER update_mltb_settings_updated_at
    BEFORE UPDATE ON mltb_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_mltb_users_updated_at ON mltb_users;
CREATE TRIGGER update_mltb_users_updated_at
    BEFORE UPDATE ON mltb_users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_mltb_rss_updated_at ON mltb_rss;
CREATE TRIGGER update_mltb_rss_updated_at
    BEFORE UPDATE ON mltb_rss
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Optional: Row Level Security (RLS) policies
-- Uncomment and modify these if you want to enable RLS

-- Enable RLS on tables
-- ALTER TABLE mltb_settings ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE mltb_users ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE mltb_rss ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE mltb_tasks ENABLE ROW LEVEL SECURITY;

-- Create policies (adjust these based on your security requirements)
-- Policy to allow service role full access (for the bot)
-- CREATE POLICY "Service role has full access" ON mltb_settings
--     FOR ALL USING (auth.role() = 'service_role');

-- CREATE POLICY "Service role has full access" ON mltb_users
--     FOR ALL USING (auth.role() = 'service_role');

-- CREATE POLICY "Service role has full access" ON mltb_rss
--     FOR ALL USING (auth.role() = 'service_role');

-- CREATE POLICY "Service role has full access" ON mltb_tasks
--     FOR ALL USING (auth.role() = 'service_role');

-- Grant permissions to the service role
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;

-- Optional: Create a view for easier monitoring
CREATE OR REPLACE VIEW mltb_database_info AS
SELECT
    'mltb_settings' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT bot_id) as unique_bots
FROM mltb_settings
UNION ALL
SELECT
    'mltb_users' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT bot_id) as unique_bots
FROM mltb_users
UNION ALL
SELECT
    'mltb_rss' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT bot_id) as unique_bots
FROM mltb_rss
UNION ALL
SELECT
    'mltb_tasks' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT bot_id) as unique_bots
FROM mltb_tasks;

-- Grant access to the view
GRANT SELECT ON mltb_database_info TO service_role;

-- Success message
SELECT 'Supabase schema for MLTBv2 Bot created successfully!' as status;