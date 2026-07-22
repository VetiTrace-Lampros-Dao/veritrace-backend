CREATE TABLE IF NOT EXISTS verified_publishers (
    creator_address VARCHAR(42) PRIMARY KEY,
    organization_name VARCHAR(100) NOT NULL,
    domain VARCHAR(100) NOT NULL,
    verified_at BIGINT NOT NULL,
    status VARCHAR(20) DEFAULT 'active'
);

-- Seed VeriTrace Official & Reuters test wallets
INSERT INTO verified_publishers (creator_address, organization_name, domain, verified_at, status)
VALUES 
('0x70997970C51812dc3A010C7d01b50e0d17dc79C8', 'VeriTrace Official', 'verify.dpkvtrading.online', 1700000000, 'active'),
('0x3C44Cd3B6aE400d39512340000000000000002ba', 'Reuters', 'reuters.com', 1700000000, 'active')
ON CONFLICT (creator_address) DO NOTHING;
