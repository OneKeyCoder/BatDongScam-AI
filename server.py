# ============================================================================
# VANNA AI - Real Estate System (RES) Data Analysis
# Kết nối cùng database PostgreSQL với Spring Boot Backend
# ============================================================================

import os
from dotenv import load_dotenv
from vanna import Agent, AgentConfig
from vanna.core.registry import ToolRegistry
from vanna.core.user import User, UserResolver
from vanna.servers.fastapi import VannaFastAPIServer
from vanna.integrations.openai import OpenAILlmService
from vanna.tools import RunSqlTool, VisualizeDataTool
from vanna.tools.agent_memory import SaveQuestionToolArgsTool, SearchSavedCorrectToolUsesTool
from vanna.integrations.local.agent_memory import DemoAgentMemory
from vanna.integrations.postgres import PostgresRunner

# Load environment variables
load_dotenv()

# ============================================================================
# Simple Anonymous User Resolver
# ============================================================================
class AnonymousUserResolver(UserResolver):
    """Simple user resolver that returns an anonymous user for all requests"""
    async def resolve_user(self, request_context):
        return User(id="anonymous", email="anonymous@localhost", group_memberships=[])

# ============================================================================
# Configuration
# ============================================================================

# LLM Service
llm = OpenAILlmService(
    model="o3",
    api_key=os.getenv("OPENAI_API_KEY"),
)

# Database Runner - Kết nối cùng PostgreSQL với Backend
db_runner = PostgresRunner(
    host=os.getenv("POSTGRES_HOST", os.getenv("POSTGRESQL_HOST", "localhost")),
    port=int(os.getenv("POSTGRES_PORT", os.getenv("POSTGRESQL_PORT", "5432"))),
    database=os.getenv("POSTGRES_DB", os.getenv("POSTGRESQL_DB", "postgres")),
    user=os.getenv("POSTGRES_USER", os.getenv("POSTGRESQL_USER", "postgres")),
    password=os.getenv("POSTGRES_PASSWORD", os.getenv("POSTGRESQL_PASSWORD", "secret"))
)

# Agent Memory
agent_memory = DemoAgentMemory(max_items=1000)

# User Resolver
user_resolver = AnonymousUserResolver()

# ============================================================================
# Tool Registry
# ============================================================================
tools = ToolRegistry()

# Database query tool
tools.register_local_tool(
    RunSqlTool(sql_runner=db_runner), 
    access_groups=[]
)

# Visualization tool
tools.register_local_tool(
    VisualizeDataTool(), 
    access_groups=[]
)

# Memory tools - Agent tự động học từ successful queries
tools.register_local_tool(
    SaveQuestionToolArgsTool(), 
    access_groups=[]
)

tools.register_local_tool(
    SearchSavedCorrectToolUsesTool(), 
    access_groups=[]
)

# ============================================================================
# Custom System Prompt - Real Estate System Schema
# ============================================================================

CUSTOM_SYSTEM_PROMPT = """
Bạn là trợ lý AI thông minh chuyên phân tích dữ liệu cho hệ thống Bất Động Sản (Real Estate System).

# QUAN TRỌNG
- Luôn trả lời bằng tiếng Việt
- Dữ liệu tiền theo VNĐ (ví dụ: 1500000000 = 1.5 tỷ VNĐ)
- Diện tích theo m² (mét vuông)
- Sử dụng PostgreSQL syntax

## DATABASE SCHEMA

### 1. USERS & AUTHENTICATION

#### Bảng users (Người dùng)
```sql
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    role VARCHAR(50) NOT NULL,  -- ADMIN, SALESAGENT, CUSTOMER, PROPERTY_OWNER, ACCOUNTANT
    email VARCHAR(255) NOT NULL UNIQUE,
    phone_number VARCHAR(20) NOT NULL UNIQUE,
    zalo_contact VARCHAR(50),
    ward_id UUID REFERENCES wards(ward_id),
    password VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    avatar_url TEXT,
    status SMALLINT NOT NULL,  -- 0=ACTIVE, 1=SUSPENDED, 2=PENDING_APPROVAL, 3=DELETED, 4=REJECTED
    identification_number VARCHAR(20),
    day_of_birth DATE,
    gender VARCHAR(10),
    nation VARCHAR(50),
    bank_account_number VARCHAR(30),
    bank_account_name VARCHAR(150),
    bank_bin VARCHAR(20),
    issue_date DATE,
    issuing_authority VARCHAR(255),
    front_id_picture_path TEXT,
    back_id_picture_path TEXT,
    last_login_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng customers (Khách hàng)
```sql
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY REFERENCES users(user_id),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng sale_agents (Nhân viên môi giới)
```sql
CREATE TABLE sale_agents (
    sale_agent_id UUID PRIMARY KEY REFERENCES users(user_id),
    employee_code VARCHAR(50) NOT NULL UNIQUE,
    max_properties INT NOT NULL,
    hired_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng property_owners (Chủ sở hữu BĐS)
```sql
CREATE TABLE property_owners (
    owner_id UUID PRIMARY KEY REFERENCES users(user_id),
    approved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 2. LOCATION (Địa điểm)

#### Bảng cities (Thành phố)
```sql
CREATE TABLE cities (
    city_id UUID PRIMARY KEY,
    city_name VARCHAR(100),
    description TEXT,
    img_url TEXT,
    total_area NUMERIC(15,2),
    avg_land_price NUMERIC(15,2),
    population INT,
    is_active BOOLEAN,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng districts (Quận/Huyện)
```sql
CREATE TABLE districts (
    district_id UUID PRIMARY KEY,
    city_id UUID NOT NULL REFERENCES cities(city_id),
    district_name VARCHAR(100),
    img_url TEXT,
    description TEXT,
    total_area NUMERIC(15,2),
    avg_land_price NUMERIC(15,2),
    population INT,
    is_active BOOLEAN,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng wards (Phường/Xã)
```sql
CREATE TABLE wards (
    ward_id UUID PRIMARY KEY,
    district_id UUID NOT NULL REFERENCES districts(district_id),
    ward_name VARCHAR(100) NOT NULL,
    img_url TEXT,
    description TEXT NOT NULL,
    total_area NUMERIC(15,2) NOT NULL,
    avg_land_price NUMERIC(15,2),
    population INT NOT NULL,
    is_active BOOLEAN,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 3. PROPERTIES (Bất động sản)

#### Bảng property_types (Loại BĐS)
```sql
CREATE TABLE property_types (
    property_type_id UUID PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,  -- Căn hộ, Nhà phố, Biệt thự, Đất nền, Shophouse...
    avatar_url TEXT,
    description TEXT,
    is_active BOOLEAN,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng properties (Bất động sản)
```sql
CREATE TABLE properties (
    property_id UUID PRIMARY KEY,
    owner_id UUID NOT NULL REFERENCES property_owners(owner_id),
    assigned_agent_id UUID REFERENCES sale_agents(sale_agent_id),
    service_fee_amount NUMERIC(15,2) NOT NULL,
    service_fee_collected_amount NUMERIC(15,2) NOT NULL,
    property_type_id UUID NOT NULL REFERENCES property_types(property_type_id),
    ward_id UUID NOT NULL REFERENCES wards(ward_id),
    title VARCHAR(200) NOT NULL,
    description TEXT NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,  -- SALE, RENTAL, INVESTMENT
    full_address TEXT,
    area NUMERIC(10,2) NOT NULL,  -- Diện tích (m²)
    rooms INT,
    bathrooms INT,
    floors INT,
    bedrooms INT,
    house_orientation VARCHAR(20),  -- NORTH, SOUTH, EAST, WEST, NORTH_EAST, NORTH_WEST, SOUTH_EAST, SOUTH_WEST
    balcony_orientation VARCHAR(20),
    year_built INT,
    price_amount NUMERIC(15,2) NOT NULL,  -- Giá (VNĐ)
    price_per_square_meter NUMERIC(15,2),
    commission_rate NUMERIC(5,4) NOT NULL,  -- Tỷ lệ hoa hồng
    amenities TEXT,  -- Tiện ích
    status VARCHAR(20),  -- PENDING, REJECTED, APPROVED, SOLD, RENTED, AVAILABLE, UNAVAILABLE, REMOVED, DELETED
    view_count INT,
    approved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng media (Ảnh/Video BĐS)
```sql
CREATE TABLE media (
    media_id UUID PRIMARY KEY,
    property_id UUID REFERENCES properties(property_id),
    report_id UUID REFERENCES violation_reports(violation_id),
    media_type VARCHAR(20) NOT NULL,  -- IMAGE, VIDEO, DOCUMENT
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    mime_type VARCHAR(100) NOT NULL,
    document_type VARCHAR(50),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 4. APPOINTMENTS (Lịch hẹn xem nhà)

```sql
CREATE TABLE appointment (
    appointment_id UUID PRIMARY KEY,
    property_id UUID NOT NULL REFERENCES properties(property_id),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    agent_id UUID REFERENCES sale_agents(sale_agent_id),
    requested_date TIMESTAMP NOT NULL,
    confirmed_date TIMESTAMP,
    status SMALLINT,  -- 0=PENDING, 1=CONFIRMED, 2=COMPLETED, 3=CANCELLED
    customer_requirements TEXT,
    agent_notes TEXT,
    viewing_outcome TEXT,
    customer_interest_level VARCHAR(20),
    cancelled_at TIMESTAMP,
    cancelled_by VARCHAR(20),  -- ADMIN, SALESAGENT, CUSTOMER, PROPERTY_OWNER
    cancelled_reason TEXT,
    rating SMALLINT,  -- 1-5
    comment TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 5. CONTRACTS (Hợp đồng)

```sql
CREATE TABLE contract (
    contract_id UUID PRIMARY KEY,
    property_id UUID NOT NULL REFERENCES properties(property_id),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    agent_id UUID NOT NULL REFERENCES sale_agents(sale_agent_id),
    contract_type VARCHAR(20) NOT NULL,  -- PURCHASE, RENTAL, INVESTMENT
    contract_number VARCHAR(50) NOT NULL UNIQUE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    special_terms TEXT NOT NULL,
    status VARCHAR(20) NOT NULL,  -- DRAFT, PENDING_SIGNING, ACTIVE, COMPLETED, CANCELLED
    cancellation_reason TEXT,
    cancellation_penalty NUMERIC(15,2),
    cancelled_by VARCHAR(20),
    contract_payment_type VARCHAR(20) NOT NULL,  -- MORTGAGE, MONTHLY_RENT, PAID_IN_FULL
    total_contract_amount NUMERIC(15,2) NOT NULL,
    deposit_amount NUMERIC(15,2) NOT NULL,
    remaining_amount NUMERIC(15,2) NOT NULL,
    advance_payment_amount NUMERIC(15,2) NOT NULL,
    installment_amount INT NOT NULL,
    progress_milestone NUMERIC(15,2) NOT NULL,
    final_payment_amount NUMERIC(15,2) NOT NULL,
    late_payment_penalty_rate NUMERIC(5,4) NOT NULL,
    special_conditions TEXT NOT NULL,
    signed_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL,
    rating SMALLINT,
    comment TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 6. PAYMENTS (Thanh toán)

```sql
CREATE TABLE payments (
    payment_id UUID PRIMARY KEY,
    contract_id UUID REFERENCES contract(contract_id),
    property_id UUID REFERENCES properties(property_id),
    sale_agent_id UUID REFERENCES sale_agents(sale_agent_id),
    payment_type VARCHAR(20) NOT NULL,  -- DEPOSIT, ADVANCE, INSTALLMENT, FULL_PAY, MONTHLY, PENALTY, REFUND, MONEY_SALE, MONEY_RENTAL, SALARY, SERVICE_FEE
    amount NUMERIC(15,2) NOT NULL,
    due_date DATE NOT NULL,
    paid_date DATE,
    installment_number INT,
    payment_method VARCHAR(50),
    transaction_reference VARCHAR(100),
    status SMALLINT,  -- 0=PENDING, 1=SUCCESS, 2=FAILED, 3=SYSTEM_PENDING, 4=SYSTEM_SUCCESS, 5=SYSTEM_FAILED
    penalty_amount NUMERIC(15,2),
    notes TEXT,
    payos_order_code BIGINT UNIQUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### ⚠️ LƯU Ý QUAN TRỌNG VỀ PAYMENTS:
- **status là SMALLINT**: 0=PENDING, 1=SUCCESS, 2=FAILED, 3=SYSTEM_PENDING, 4=SYSTEM_SUCCESS, 5=SYSTEM_FAILED
- **payment_type là STRING**: DEPOSIT, ADVANCE, INSTALLMENT, FULL_PAY, MONTHLY, PENALTY, REFUND, MONEY_SALE, MONEY_RENTAL, SALARY, SERVICE_FEE
- **Ý nghĩa các payment_type:**
  - SALARY: Tiền lương trả cho nhân viên môi giới
  - DEPOSIT: Tiền đặt cọc từ khách hàng
  - ADVANCE: Tiền thanh toán trước/tạm ứng
  - INSTALLMENT: Tiền trả góp theo kỳ
  - FULL_PAY: Thanh toán toàn bộ một lần
  - MONTHLY: Tiền thuê hàng tháng
  - PENALTY: Tiền phạt vi phạm hợp đồng
  - REFUND: Tiền hoàn trả
  - MONEY_SALE: Tiền chủ nhà nhận được từ việc bán BĐS
  - MONEY_RENTAL: Tiền chủ nhà nhận được từ cho thuê BĐS
  - SERVICE_FEE: Phí dịch vụ chủ nhà trả cho hệ thống

### 7. DOCUMENTS (Giấy tờ pháp lý)

#### Bảng document_types
```sql
CREATE TABLE document_types (
    document_type_id UUID PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    is_compulsory BOOLEAN,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

#### Bảng identification_documents
```sql
CREATE TABLE identification_documents (
    document_id UUID PRIMARY KEY,
    document_type_id UUID NOT NULL REFERENCES document_types(document_type_id),
    property_id UUID REFERENCES properties(property_id),
    document_number VARCHAR(20) NOT NULL,
    document_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    issue_date DATE,
    expiry_date DATE,
    issuing_authority VARCHAR(100),
    verification_status VARCHAR(20),  -- PENDING, VERIFIED, REJECTED
    verified_at TIMESTAMP,
    rejection_reason TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 8. NOTIFICATIONS (Thông báo)

```sql
CREATE TABLE notifications (
    notification_id UUID PRIMARY KEY,
    recipient_id UUID NOT NULL REFERENCES users(user_id),
    type VARCHAR(30),  -- APPOINTMENT_REMINDER, CONTRACT_UPDATE, PAYMENT_DUE, VIOLATION_WARNING, SYSTEM_ALERT
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    related_entity_type VARCHAR(20),  -- PROPERTY, CONTRACT, PAYMENT, APPOINTMENT, USER
    related_entity_id VARCHAR(100),
    delivery_status VARCHAR(20),  -- PENDING, SENT, READ, FAILED
    is_read BOOLEAN,
    img_url TEXT NOT NULL,
    read_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 9. VIOLATION REPORTS (Báo cáo vi phạm)

```sql
CREATE TABLE violation_reports (
    violation_id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(user_id),
    related_entity_type VARCHAR(20) NOT NULL,  -- CUSTOMER, PROPERTY, SALES_AGENT, PROPERTY_OWNER
    related_entity_id UUID NOT NULL,
    violation_type VARCHAR(50) NOT NULL,  -- FRAUDULENT_LISTING, MISREPRESENTATION_OF_PROPERTY, SPAM_OR_DUPLICATE_LISTING, INAPPROPRIATE_CONTENT, NON_COMPLIANCE_WITH_TERMS, FAILURE_TO_DISCLOSE_INFORMATION, HARASSMENT, SCAM_ATTEMPT
    description TEXT NOT NULL,
    status VARCHAR(20),  -- PENDING, REPORTED, UNDER_REVIEW, RESOLVED, DISMISSED
    resolution_notes TEXT,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

## HƯỚNG DẪN TẠO SQL

1. **Luôn dùng PostgreSQL syntax**
2. **UUID là kiểu dữ liệu cho các ID**
3. **Ghép họ tên:** `CONCAT(last_name, ' ', first_name) AS full_name`
4. **Format tiền VNĐ:** Giữ nguyên số, giải thích khi trả lời (ví dụ: 1.5 tỷ)
5. **Join bảng vị trí:** properties → wards → districts → cities
6. **Join bảng user liên quan:** customers/sale_agents/property_owners → users

## ⚠️ QUAN TRỌNG: STATUS MAPPING

Một số bảng dùng SMALLINT cho status (lưu dạng số), một số dùng VARCHAR (lưu dạng string):

### Bảng dùng SMALLINT (số):
- **users.status**: 0=ACTIVE, 1=SUSPENDED, 2=PENDING_APPROVAL, 3=DELETED, 4=REJECTED
- **appointment.status**: 0=PENDING, 1=CONFIRMED, 2=COMPLETED, 3=CANCELLED  
- **payments.status**: 0=PENDING, 1=SUCCESS, 2=FAILED, 3=SYSTEM_PENDING, 4=SYSTEM_SUCCESS, 5=SYSTEM_FAILED

### Bảng dùng VARCHAR (string):
- **properties.status**: 'PENDING', 'REJECTED', 'APPROVED', 'SOLD', 'RENTED', 'AVAILABLE', 'UNAVAILABLE', 'REMOVED', 'DELETED'
- **contract.status**: 'DRAFT', 'PENDING_SIGNING', 'ACTIVE', 'COMPLETED', 'CANCELLED'
- **violation_reports.status**: 'PENDING', 'REPORTED', 'UNDER_REVIEW', 'RESOLVED', 'DISMISSED'

## ⚠️ QUAN TRỌNG: CÁCH JOIN ĐỂ PHÂN TÍCH KHÁCH HÀNG CHI TIÊU

Để biết khách hàng chi tiêu bao nhiêu, cần join: **payments → contract → customers → users**
```sql
-- Ví dụ: Khách hàng chi tiêu nhiều nhất
SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name,
       u.email, u.gender, u.day_of_birth,
       SUM(p.amount) AS total_spent
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1  -- SUCCESS
GROUP BY c.customer_id, u.last_name, u.first_name, u.email, u.gender, u.day_of_birth
ORDER BY total_spent DESC;
```

Để biết khu vực khách hàng sinh sống, thêm join: **users → wards → districts → cities**

## EXAMPLE QUERIES

### Tổng số BĐS theo trạng thái
```sql
SELECT status, COUNT(*) as so_luong
FROM properties
GROUP BY status
ORDER BY so_luong DESC;
```

### Danh sách BĐS đang bán/cho thuê có giá cao nhất
```sql
SELECT p.title, p.price_amount, p.area, p.transaction_type,
       pt.type_name, c.city_name, d.district_name
FROM properties p
JOIN property_types pt ON p.property_type_id = pt.property_type_id
JOIN wards w ON p.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities c ON d.city_id = c.city_id
WHERE p.status = 'AVAILABLE'
ORDER BY p.price_amount DESC
LIMIT 10;
```

### Top 10 nhân viên môi giới có nhiều BĐS nhất
```sql
SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name,
       sa.employee_code,
       COUNT(p.property_id) AS total_properties
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
LEFT JOIN properties p ON p.assigned_agent_id = sa.sale_agent_id
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
ORDER BY total_properties DESC
LIMIT 10;
```

### Doanh thu theo tháng (từ payments thành công)
```sql
SELECT DATE_TRUNC('month', paid_date) AS thang,
       SUM(amount) AS tong_doanh_thu
FROM payments
WHERE status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;
```

### Số lượng lịch hẹn theo trạng thái
```sql
SELECT status, COUNT(*) AS so_luong
FROM appointment
GROUP BY status;
```

### Thống kê hợp đồng theo loại
```sql
SELECT contract_type, status, COUNT(*) AS so_luong,
       SUM(total_contract_amount) AS tong_gia_tri
FROM contract
GROUP BY contract_type, status
ORDER BY contract_type, status;
```

### BĐS theo khu vực (thành phố)
```sql
SELECT c.city_name, COUNT(p.property_id) AS so_bds,
       AVG(p.price_amount) AS gia_trung_binh,
       AVG(p.area) AS dien_tich_tb
FROM properties p
JOIN wards w ON p.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities c ON d.city_id = c.city_id
WHERE p.status IN ('AVAILABLE', 'APPROVED')
GROUP BY c.city_id, c.city_name
ORDER BY so_bds DESC;
```

### Khách hàng có nhiều lịch hẹn nhất
```sql
SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name,
       u.email, u.phone_number,
       COUNT(a.appointment_id) AS total_appointments
FROM customers cu
JOIN users u ON cu.customer_id = u.user_id
LEFT JOIN appointment a ON a.customer_id = cu.customer_id
GROUP BY cu.customer_id, u.last_name, u.first_name, u.email, u.phone_number
ORDER BY total_appointments DESC
LIMIT 10;
```

### Chủ sở hữu có nhiều BĐS nhất
```sql
SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name,
       u.email, COUNT(p.property_id) AS total_properties,
       SUM(p.price_amount) AS total_value
FROM property_owners po
JOIN users u ON po.owner_id = u.user_id
LEFT JOIN properties p ON p.owner_id = po.owner_id
GROUP BY po.owner_id, u.last_name, u.first_name, u.email
ORDER BY total_properties DESC
LIMIT 10;
```

### Rating trung bình của các nhân viên
```sql
SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name,
       sa.employee_code,
       AVG(a.rating) AS avg_appointment_rating,
       AVG(c.rating) AS avg_contract_rating,
       COUNT(DISTINCT a.appointment_id) AS total_appointments,
       COUNT(DISTINCT c.contract_id) AS total_contracts
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
LEFT JOIN appointment a ON a.agent_id = sa.sale_agent_id AND a.rating IS NOT NULL
LEFT JOIN contract c ON c.agent_id = sa.sale_agent_id AND c.rating IS NOT NULL
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
HAVING AVG(a.rating) IS NOT NULL OR AVG(c.rating) IS NOT NULL
ORDER BY avg_appointment_rating DESC NULLS LAST;
```

## NHIỆM VỤ CỦA BẠN

1. Hiểu câu hỏi của người dùng về hệ thống bất động sản
2. Tạo SQL query chính xác dựa trên schema trên
3. Chạy query và trả về kết quả
4. Nếu cần, tạo visualization (biểu đồ) để minh họa
5. Giải thích kết quả bằng tiếng Việt dễ hiểu

### Gợi ý theo từ khóa:
- "BĐS/Bất động sản/Nhà/Căn hộ" → Bảng `properties`
- "Khách hàng" → Bảng `customers` JOIN `users`
- "Nhân viên/Môi giới/Agent" → Bảng `sale_agents` JOIN `users`
- "Chủ nhà/Chủ sở hữu" → Bảng `property_owners` JOIN `users`
- "Lịch hẹn/Xem nhà" → Bảng `appointment`
- "Hợp đồng" → Bảng `contract`
- "Thanh toán/Doanh thu" → Bảng `payments`
- "Thành phố/Quận/Phường" → Bảng `cities`, `districts`, `wards`
- "Vi phạm/Báo cáo" → Bảng `violation_reports`
"""

# Create agent with custom system prompt
agent = Agent(
    llm_service=llm,
    tool_registry=tools,
    user_resolver=user_resolver,
    agent_memory=agent_memory,
    config=AgentConfig(
        system_prompt=CUSTOM_SYSTEM_PROMPT,
        max_tool_iterations=100,
        temperature=0.1
    )
)

# ============================================================================
# Pre-populate Agent Memory với training data
# ============================================================================

async def populate_memory():
    """Pre-populate agent memory with common query patterns for Real Estate System"""
    from vanna.core.tool import ToolContext
    from vanna.core.user import User
    
    mock_user = User(id="system", email="system@vanna.ai", group_memberships=[])
    mock_context = ToolContext(
        user=mock_user,
        agent_memory=agent_memory,
        conversation_id="training",
        message_id="training",
        request_id="training-request"
    )
    
    # Training data - Common question-SQL pairs for Real Estate System
    training_data = [
        # ==================== PROPERTIES ====================
        {
            "question": "Có bao nhiêu bất động sản trong hệ thống?",
            "sql": "SELECT COUNT(*) AS total_properties FROM properties;"
        },
        {
            "question": "Thống kê BĐS theo trạng thái",
            "sql": "SELECT status, COUNT(*) AS so_luong FROM properties GROUP BY status ORDER BY so_luong DESC;"
        },
        {
            "question": "Top 10 BĐS giá cao nhất đang bán",
            "sql": """SELECT p.title, p.price_amount, p.area, pt.type_name, c.city_name
FROM properties p
JOIN property_types pt ON p.property_type_id = pt.property_type_id
JOIN wards w ON p.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities c ON d.city_id = c.city_id
WHERE p.status = 'AVAILABLE' AND p.transaction_type = 'SALE'
ORDER BY p.price_amount DESC LIMIT 10;"""
        },
        {
            "question": "BĐS cho thuê có giá thấp nhất",
            "sql": """SELECT p.title, p.price_amount, p.area, pt.type_name, c.city_name, d.district_name
FROM properties p
JOIN property_types pt ON p.property_type_id = pt.property_type_id
JOIN wards w ON p.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities c ON d.city_id = c.city_id
WHERE p.status = 'AVAILABLE' AND p.transaction_type = 'RENTAL'
ORDER BY p.price_amount ASC LIMIT 10;"""
        },
        {
            "question": "BĐS theo loại giao dịch",
            "sql": "SELECT transaction_type, COUNT(*) AS so_luong, AVG(price_amount) AS gia_tb FROM properties GROUP BY transaction_type;"
        },
        {
            "question": "BĐS theo hướng nhà",
            "sql": "SELECT house_orientation, COUNT(*) AS so_luong FROM properties WHERE house_orientation IS NOT NULL GROUP BY house_orientation ORDER BY so_luong DESC;"
        },
        {
            "question": "BĐS có diện tích lớn nhất",
            "sql": "SELECT title, area, price_amount, full_address FROM properties ORDER BY area DESC LIMIT 10;"
        },
        {
            "question": "BĐS mới đăng trong tháng này",
            "sql": "SELECT title, price_amount, transaction_type, status, created_at FROM properties WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', CURRENT_DATE) ORDER BY created_at DESC;"
        },
        {
            "question": "BĐS đã được duyệt",
            "sql": "SELECT COUNT(*) AS approved_count FROM properties WHERE status = 'APPROVED' OR approved_at IS NOT NULL;"
        },
        {
            "question": "Giá trung bình BĐS theo loại",
            "sql": """SELECT pt.type_name, COUNT(p.property_id) AS so_bds, AVG(p.price_amount) AS gia_tb, AVG(p.area) AS dien_tich_tb
FROM properties p
JOIN property_types pt ON p.property_type_id = pt.property_type_id
GROUP BY pt.property_type_id, pt.type_name ORDER BY gia_tb DESC;"""
        },
        
        # ==================== PROPERTY TYPES ====================
        {
            "question": "Danh sách loại BĐS",
            "sql": "SELECT type_name, description, is_active FROM property_types ORDER BY type_name;"
        },
        {
            "question": "Loại BĐS nào phổ biến nhất?",
            "sql": """SELECT pt.type_name, COUNT(p.property_id) AS so_bds
FROM property_types pt
LEFT JOIN properties p ON p.property_type_id = pt.property_type_id
GROUP BY pt.property_type_id, pt.type_name ORDER BY so_bds DESC;"""
        },
        
        # ==================== USERS ====================
        {
            "question": "Có bao nhiêu người dùng?",
            "sql": "SELECT COUNT(*) AS total_users FROM users;"
        },
        {
            "question": "Thống kê người dùng theo vai trò",
            "sql": "SELECT role, COUNT(*) AS so_luong FROM users GROUP BY role ORDER BY so_luong DESC;"
        },
        {
            "question": "Thống kê người dùng theo trạng thái",
            "sql": "SELECT status, COUNT(*) AS so_luong FROM users GROUP BY status ORDER BY so_luong DESC;"
        },
        {
            "question": "Người dùng đăng ký mới trong tháng",
            "sql": "SELECT role, COUNT(*) AS so_luong FROM users WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', CURRENT_DATE) GROUP BY role;"
        },
        {
            "question": "Người dùng theo giới tính",
            "sql": "SELECT gender, COUNT(*) AS so_luong FROM users WHERE gender IS NOT NULL GROUP BY gender;"
        },
        
        # ==================== CUSTOMERS ====================
        {
            "question": "Có bao nhiêu khách hàng?",
            "sql": "SELECT COUNT(*) AS total_customers FROM customers;"
        },
        {
            "question": "Danh sách khách hàng",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, u.email, u.phone_number, u.status
FROM customers cu
JOIN users u ON cu.customer_id = u.user_id
ORDER BY u.created_at DESC LIMIT 20;"""
        },
        {
            "question": "Khách hàng có nhiều lịch hẹn nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, u.email, u.phone_number, COUNT(a.appointment_id) AS total_appointments
FROM customers cu
JOIN users u ON cu.customer_id = u.user_id
LEFT JOIN appointment a ON a.customer_id = cu.customer_id
GROUP BY cu.customer_id, u.last_name, u.first_name, u.email, u.phone_number
ORDER BY total_appointments DESC LIMIT 10;"""
        },
        {
            "question": "Khách hàng có nhiều hợp đồng nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, u.email, COUNT(c.contract_id) AS total_contracts, SUM(c.total_contract_amount) AS total_value
FROM customers cu
JOIN users u ON cu.customer_id = u.user_id
LEFT JOIN contract c ON c.customer_id = cu.customer_id
GROUP BY cu.customer_id, u.last_name, u.first_name, u.email
ORDER BY total_contracts DESC LIMIT 10;"""
        },
        
        # === PHÂN TÍCH KHÁCH HÀNG CHI TIÊU ===
        {
            "question": "Khách hàng nào chi tiêu nhiều nhất?",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, u.email, u.phone_number,
       SUM(p.amount) AS total_spent, COUNT(DISTINCT ct.contract_id) AS so_hop_dong
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1
GROUP BY c.customer_id, u.last_name, u.first_name, u.email, u.phone_number
ORDER BY total_spent DESC LIMIT 10;"""
        },
        {
            "question": "Khách hàng chi tiêu nhiều tiền nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, u.email, u.phone_number,
       SUM(p.amount) AS total_spent, COUNT(DISTINCT ct.contract_id) AS so_hop_dong
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1
GROUP BY c.customer_id, u.last_name, u.first_name, u.email, u.phone_number
ORDER BY total_spent DESC LIMIT 10;"""
        },
        {
            "question": "Top khách hàng VIP chi tiêu cao",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, u.email, u.phone_number, u.gender,
       EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) AS tuoi,
       SUM(p.amount) AS total_spent, COUNT(DISTINCT ct.contract_id) AS so_hop_dong
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1
GROUP BY c.customer_id, u.last_name, u.first_name, u.email, u.phone_number, u.gender, u.day_of_birth
ORDER BY total_spent DESC LIMIT 20;"""
        },
        {
            "question": "Phân tích khách hàng theo giới tính",
            "sql": """SELECT u.gender AS gioi_tinh, 
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu,
       AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1 AND u.gender IS NOT NULL
GROUP BY u.gender
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Khách hàng chi tiêu theo giới tính",
            "sql": """SELECT u.gender AS gioi_tinh, 
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu,
       AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1 AND u.gender IS NOT NULL
GROUP BY u.gender
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Phân tích khách hàng theo độ tuổi",
            "sql": """SELECT 
    CASE 
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 25 THEN 'Dưới 25 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 35 THEN '25-34 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 45 THEN '35-44 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 55 THEN '45-54 tuổi'
        ELSE 'Trên 55 tuổi'
    END AS nhom_tuoi,
    COUNT(DISTINCT c.customer_id) AS so_khach_hang,
    SUM(p.amount) AS tong_chi_tieu,
    AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1 AND u.day_of_birth IS NOT NULL
GROUP BY nhom_tuoi
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Khách hàng chi tiêu theo độ tuổi",
            "sql": """SELECT 
    CASE 
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 25 THEN 'Dưới 25 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 35 THEN '25-34 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 45 THEN '35-44 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 55 THEN '45-54 tuổi'
        ELSE 'Trên 55 tuổi'
    END AS nhom_tuoi,
    COUNT(DISTINCT c.customer_id) AS so_khach_hang,
    SUM(p.amount) AS tong_chi_tieu,
    AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1 AND u.day_of_birth IS NOT NULL
GROUP BY nhom_tuoi
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Phân tích khách hàng theo khu vực sinh sống",
            "sql": """SELECT ci.city_name AS thanh_pho,
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu,
       AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
JOIN wards w ON u.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities ci ON d.city_id = ci.city_id
WHERE p.status = 1
GROUP BY ci.city_id, ci.city_name
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Khách hàng chi tiêu theo thành phố",
            "sql": """SELECT ci.city_name AS thanh_pho,
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu,
       AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
JOIN wards w ON u.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities ci ON d.city_id = ci.city_id
WHERE p.status = 1
GROUP BY ci.city_id, ci.city_name
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Khách hàng chi tiêu theo quận huyện",
            "sql": """SELECT ci.city_name AS thanh_pho, d.district_name AS quan_huyen,
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
JOIN wards w ON u.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities ci ON d.city_id = ci.city_id
WHERE p.status = 1
GROUP BY ci.city_name, d.district_id, d.district_name
ORDER BY tong_chi_tieu DESC LIMIT 20;"""
        },
        {
            "question": "Tệp khách hàng tiềm năng",
            "sql": """SELECT u.gender AS gioi_tinh,
    CASE 
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 25 THEN 'Dưới 25 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 35 THEN '25-34 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 45 THEN '35-44 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 55 THEN '45-54 tuổi'
        ELSE 'Trên 55 tuổi'
    END AS nhom_tuoi,
    COUNT(DISTINCT c.customer_id) AS so_khach_hang,
    SUM(p.amount) AS tong_chi_tieu,
    AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1 AND u.gender IS NOT NULL AND u.day_of_birth IS NOT NULL
GROUP BY u.gender, nhom_tuoi
ORDER BY tong_chi_tieu DESC;"""
        },
        {
            "question": "Phân tích chân dung khách hàng",
            "sql": """SELECT u.gender AS gioi_tinh,
    CASE 
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 25 THEN 'Dưới 25 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 35 THEN '25-34 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 45 THEN '35-44 tuổi'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) < 55 THEN '45-54 tuổi'
        ELSE 'Trên 55 tuổi'
    END AS nhom_tuoi,
    ci.city_name AS thanh_pho,
    COUNT(DISTINCT c.customer_id) AS so_khach_hang,
    SUM(p.amount) AS tong_chi_tieu
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
LEFT JOIN wards w ON u.ward_id = w.ward_id
LEFT JOIN districts d ON w.district_id = d.district_id
LEFT JOIN cities ci ON d.city_id = ci.city_id
WHERE p.status = 1
GROUP BY u.gender, nhom_tuoi, ci.city_name
ORDER BY tong_chi_tieu DESC LIMIT 20;"""
        },
        {
            "question": "Khách hàng VIP cần quan tâm",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS customer_name, 
       u.email, u.phone_number, u.gender AS gioi_tinh,
       EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.day_of_birth) AS tuoi,
       ci.city_name AS thanh_pho,
       SUM(p.amount) AS tong_chi_tieu,
       COUNT(DISTINCT ct.contract_id) AS so_hop_dong
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
LEFT JOIN wards w ON u.ward_id = w.ward_id
LEFT JOIN districts d ON w.district_id = d.district_id
LEFT JOIN cities ci ON d.city_id = ci.city_id
WHERE p.status = 1
GROUP BY c.customer_id, u.last_name, u.first_name, u.email, u.phone_number, u.gender, u.day_of_birth, ci.city_name
ORDER BY tong_chi_tieu DESC LIMIT 10;"""
        },
        {
            "question": "Phân tích khách hàng theo loại giao dịch",
            "sql": """SELECT ct.contract_type AS loai_giao_dich,
       u.gender AS gioi_tinh,
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
JOIN users u ON c.customer_id = u.user_id
WHERE p.status = 1
GROUP BY ct.contract_type, u.gender
ORDER BY ct.contract_type, tong_chi_tieu DESC;"""
        },
        {
            "question": "Khách hàng mua nhà vs thuê nhà",
            "sql": """SELECT ct.contract_type AS loai_giao_dich,
       COUNT(DISTINCT c.customer_id) AS so_khach_hang,
       SUM(p.amount) AS tong_chi_tieu,
       AVG(p.amount) AS chi_tieu_tb
FROM payments p
JOIN contract ct ON p.contract_id = ct.contract_id
JOIN customers c ON ct.customer_id = c.customer_id
WHERE p.status = 1
GROUP BY ct.contract_type
ORDER BY tong_chi_tieu DESC;"""
        },
        
        # ==================== SALE AGENTS ====================
        {
            "question": "Có bao nhiêu nhân viên môi giới?",
            "sql": "SELECT COUNT(*) AS total_agents FROM sale_agents;"
        },
        {
            "question": "Danh sách nhân viên môi giới",
            "sql": """SELECT sa.employee_code, CONCAT(u.last_name, ' ', u.first_name) AS agent_name, u.email, u.phone_number, sa.max_properties, sa.hired_date
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
ORDER BY sa.hired_date DESC;"""
        },
        {
            "question": "Nhân viên nào có nhiều BĐS nhất?",
            "sql": """SELECT sa.employee_code, CONCAT(u.last_name, ' ', u.first_name) AS agent_name, COUNT(p.property_id) AS total_properties
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
LEFT JOIN properties p ON p.assigned_agent_id = sa.sale_agent_id
GROUP BY sa.sale_agent_id, sa.employee_code, u.last_name, u.first_name
ORDER BY total_properties DESC LIMIT 10;"""
        },
        {
            "question": "Nhân viên nào có nhiều hợp đồng nhất?",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name, sa.employee_code, COUNT(c.contract_id) AS total_contracts
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
LEFT JOIN contract c ON c.agent_id = sa.sale_agent_id
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
ORDER BY total_contracts DESC LIMIT 10;"""
        },
        {
            "question": "Rating trung bình của nhân viên",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name, sa.employee_code, AVG(a.rating) AS avg_rating, COUNT(a.appointment_id) AS total_rated
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
JOIN appointment a ON a.agent_id = sa.sale_agent_id AND a.rating IS NOT NULL
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
ORDER BY avg_rating DESC;"""
        },
        {
            "question": "Nhân viên mới được tuyển trong năm nay",
            "sql": """SELECT sa.employee_code, CONCAT(u.last_name, ' ', u.first_name) AS agent_name, sa.hired_date
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
WHERE EXTRACT(YEAR FROM sa.hired_date) = EXTRACT(YEAR FROM CURRENT_DATE)
ORDER BY sa.hired_date DESC;"""
        },
        
        # ==================== PROPERTY OWNERS ====================
        {
            "question": "Có bao nhiêu chủ sở hữu BĐS?",
            "sql": "SELECT COUNT(*) AS total_owners FROM property_owners;"
        },
        {
            "question": "Chủ sở hữu có nhiều BĐS nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, u.email, COUNT(p.property_id) AS total_properties, SUM(p.price_amount) AS total_value
FROM property_owners po
JOIN users u ON po.owner_id = u.user_id
LEFT JOIN properties p ON p.owner_id = po.owner_id
GROUP BY po.owner_id, u.last_name, u.first_name, u.email
ORDER BY total_properties DESC LIMIT 10;"""
        },
        {
            "question": "Chủ sở hữu chờ duyệt",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, u.email, u.phone_number, u.created_at
FROM property_owners po
JOIN users u ON po.owner_id = u.user_id
WHERE po.approved_at IS NULL
ORDER BY u.created_at DESC;"""
        },
        
        # ==================== APPOINTMENTS ====================
        {
            "question": "Có bao nhiêu lịch hẹn?",
            "sql": "SELECT COUNT(*) AS total_appointments FROM appointment;"
        },
        {
            "question": "Số lượng lịch hẹn theo trạng thái",
            "sql": "SELECT status, COUNT(*) AS so_luong FROM appointment GROUP BY status ORDER BY so_luong DESC;"
        },
        {
            "question": "Lịch hẹn hôm nay",
            "sql": "SELECT * FROM appointment WHERE DATE(requested_date) = CURRENT_DATE ORDER BY requested_date;"
        },
        {
            "question": "Lịch hẹn tuần này",
            "sql": "SELECT status, COUNT(*) AS so_luong FROM appointment WHERE DATE_TRUNC('week', requested_date) = DATE_TRUNC('week', CURRENT_DATE) GROUP BY status;"
        },
        {
            "question": "Lịch hẹn bị hủy nhiều nhất",
            "sql": "SELECT cancelled_by, cancelled_reason, COUNT(*) AS so_luong FROM appointment WHERE status = 3 GROUP BY cancelled_by, cancelled_reason ORDER BY so_luong DESC LIMIT 10;"
        },
        {
            "question": "Rating lịch hẹn trung bình",
            "sql": "SELECT AVG(rating) AS avg_rating, COUNT(*) AS total_rated FROM appointment WHERE rating IS NOT NULL;"
        },
        
        # ==================== CONTRACTS ====================
        {
            "question": "Có bao nhiêu hợp đồng?",
            "sql": "SELECT COUNT(*) AS total_contracts FROM contract;"
        },
        {
            "question": "Thống kê hợp đồng theo loại",
            "sql": """SELECT contract_type, status, COUNT(*) AS so_luong, SUM(total_contract_amount) AS tong_gia_tri
FROM contract GROUP BY contract_type, status ORDER BY contract_type, status;"""
        },
        {
            "question": "Hợp đồng đang hoạt động",
            "sql": "SELECT contract_number, contract_type, total_contract_amount, start_date, end_date FROM contract WHERE status = 'ACTIVE' ORDER BY start_date DESC;"
        },
        {
            "question": "Tổng giá trị hợp đồng theo tháng",
            "sql": """SELECT DATE_TRUNC('month', signed_at) AS thang, COUNT(*) AS so_hop_dong, SUM(total_contract_amount) AS tong_gia_tri
FROM contract
WHERE signed_at IS NOT NULL
GROUP BY DATE_TRUNC('month', signed_at)
ORDER BY thang DESC;"""
        },
        {
            "question": "Hợp đồng sắp hết hạn",
            "sql": "SELECT contract_number, contract_type, end_date, total_contract_amount FROM contract WHERE status = 'ACTIVE' AND end_date <= CURRENT_DATE + INTERVAL '30 days' ORDER BY end_date;"
        },
        
        # ==================== PAYMENTS ====================
        {
            "question": "Có bao nhiêu giao dịch thanh toán?",
            "sql": "SELECT COUNT(*) AS total_payments FROM payments;"
        },
        {
            "question": "Doanh thu tháng này",
            "sql": """SELECT SUM(amount) AS doanh_thu
FROM payments
WHERE status = 1 AND DATE_TRUNC('month', paid_date) = DATE_TRUNC('month', CURRENT_DATE);"""
        },
        {
            "question": "Doanh thu theo tháng",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, SUM(amount) AS tong_doanh_thu, COUNT(*) AS so_giao_dich
FROM payments
WHERE status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        {
            "question": "Thanh toán theo loại",
            "sql": "SELECT payment_type, COUNT(*) AS so_luong, SUM(amount) AS tong_tien FROM payments GROUP BY payment_type ORDER BY tong_tien DESC;"
        },
        {
            "question": "Thanh toán theo trạng thái",
            "sql": "SELECT status, COUNT(*) AS so_luong, SUM(amount) AS tong_tien FROM payments GROUP BY status ORDER BY so_luong DESC;"
        },
        {
            "question": "Thanh toán quá hạn",
            "sql": "SELECT * FROM payments WHERE status = 0 AND due_date < CURRENT_DATE ORDER BY due_date;"
        },
        {
            "question": "Phương thức thanh toán phổ biến",
            "sql": "SELECT payment_method, COUNT(*) AS so_luong, SUM(amount) AS tong_tien FROM payments WHERE payment_method IS NOT NULL GROUP BY payment_method ORDER BY so_luong DESC;"
        },
        
        # === SALARY - Tiền lương nhân viên môi giới ===
        {
            "question": "Tổng tiền lương nhân viên",
            "sql": "SELECT SUM(amount) AS tong_luong FROM payments WHERE payment_type = 'SALARY';"
        },
        {
            "question": "Tiền lương đã thanh toán cho nhân viên",
            "sql": "SELECT SUM(amount) AS tong_luong_da_tra FROM payments WHERE payment_type = 'SALARY' AND status = 1;"
        },
        {
            "question": "Tiền lương chưa thanh toán cho nhân viên",
            "sql": "SELECT SUM(amount) AS tong_luong_chua_tra FROM payments WHERE payment_type = 'SALARY' AND status = 0;"
        },
        {
            "question": "Lương nhân viên theo tháng",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, SUM(amount) AS tong_luong, COUNT(*) AS so_nhan_vien
FROM payments
WHERE payment_type = 'SALARY' AND status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        {
            "question": "Chi tiết lương từng nhân viên",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name, sa.employee_code, 
       SUM(p.amount) AS tong_luong, COUNT(p.payment_id) AS so_lan_nhan_luong
FROM payments p
JOIN sale_agents sa ON p.sale_agent_id = sa.sale_agent_id
JOIN users u ON sa.sale_agent_id = u.user_id
WHERE p.payment_type = 'SALARY'
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
ORDER BY tong_luong DESC;"""
        },
        {
            "question": "Nhân viên có lương cao nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name, sa.employee_code, 
       SUM(p.amount) AS tong_luong
FROM payments p
JOIN sale_agents sa ON p.sale_agent_id = sa.sale_agent_id
JOIN users u ON sa.sale_agent_id = u.user_id
WHERE p.payment_type = 'SALARY' AND p.status = 1
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
ORDER BY tong_luong DESC LIMIT 10;"""
        },
        {
            "question": "Lương nhân viên chưa được trả",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS agent_name, sa.employee_code, 
       p.amount, p.due_date, p.notes
FROM payments p
JOIN sale_agents sa ON p.sale_agent_id = sa.sale_agent_id
JOIN users u ON sa.sale_agent_id = u.user_id
WHERE p.payment_type = 'SALARY' AND p.status = 0
ORDER BY p.due_date;"""
        },
        
        # === DEPOSIT - Tiền đặt cọc từ khách hàng ===
        {
            "question": "Tổng tiền đặt cọc",
            "sql": "SELECT SUM(amount) AS tong_dat_coc FROM payments WHERE payment_type = 'DEPOSIT';"
        },
        {
            "question": "Tiền đặt cọc đã nhận",
            "sql": "SELECT SUM(amount) AS tong_dat_coc_da_nhan FROM payments WHERE payment_type = 'DEPOSIT' AND status = 1;"
        },
        {
            "question": "Tiền đặt cọc theo hợp đồng",
            "sql": """SELECT c.contract_number, c.contract_type, p.amount AS tien_dat_coc, p.paid_date, p.status
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'DEPOSIT'
ORDER BY p.created_at DESC;"""
        },
        {
            "question": "Đặt cọc chờ thanh toán",
            "sql": """SELECT c.contract_number, p.amount, p.due_date
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'DEPOSIT' AND p.status = 0
ORDER BY p.due_date;"""
        },
        
        # === ADVANCE - Tiền thanh toán trước/tạm ứng ===
        {
            "question": "Tổng tiền tạm ứng",
            "sql": "SELECT SUM(amount) AS tong_tam_ung FROM payments WHERE payment_type = 'ADVANCE';"
        },
        {
            "question": "Tiền tạm ứng đã nhận",
            "sql": "SELECT SUM(amount) AS tong_tam_ung_da_nhan FROM payments WHERE payment_type = 'ADVANCE' AND status = 1;"
        },
        {
            "question": "Chi tiết thanh toán tạm ứng theo hợp đồng",
            "sql": """SELECT c.contract_number, c.contract_type, p.amount AS tien_tam_ung, p.paid_date, p.status
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'ADVANCE'
ORDER BY p.created_at DESC;"""
        },
        
        # === INSTALLMENT - Tiền trả góp theo kỳ ===
        {
            "question": "Tổng tiền trả góp",
            "sql": "SELECT SUM(amount) AS tong_tra_gop FROM payments WHERE payment_type = 'INSTALLMENT';"
        },
        {
            "question": "Tiền trả góp đã nhận",
            "sql": "SELECT SUM(amount) AS tong_tra_gop_da_nhan FROM payments WHERE payment_type = 'INSTALLMENT' AND status = 1;"
        },
        {
            "question": "Chi tiết các kỳ trả góp",
            "sql": """SELECT c.contract_number, p.installment_number AS ky_tra_gop, p.amount, p.due_date, p.paid_date, p.status
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'INSTALLMENT'
ORDER BY c.contract_number, p.installment_number;"""
        },
        {
            "question": "Kỳ trả góp quá hạn",
            "sql": """SELECT c.contract_number, p.installment_number AS ky_tra_gop, p.amount, p.due_date
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'INSTALLMENT' AND p.status = 0 AND p.due_date < CURRENT_DATE
ORDER BY p.due_date;"""
        },
        {
            "question": "Thống kê trả góp theo hợp đồng",
            "sql": """SELECT c.contract_number, c.contract_type, 
       COUNT(p.payment_id) AS so_ky, 
       SUM(CASE WHEN p.status = 1 THEN 1 ELSE 0 END) AS ky_da_tra,
       SUM(CASE WHEN p.status = 0 THEN 1 ELSE 0 END) AS ky_chua_tra,
       SUM(p.amount) AS tong_tien
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'INSTALLMENT'
GROUP BY c.contract_id, c.contract_number, c.contract_type;"""
        },
        
        # === FULL_PAY - Thanh toán toàn bộ một lần ===
        {
            "question": "Tổng thanh toán một lần",
            "sql": "SELECT SUM(amount) AS tong_thanh_toan_1_lan FROM payments WHERE payment_type = 'FULL_PAY';"
        },
        {
            "question": "Hợp đồng thanh toán một lần",
            "sql": """SELECT c.contract_number, c.contract_type, p.amount, p.paid_date, p.status
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'FULL_PAY'
ORDER BY p.amount DESC;"""
        },
        
        # === MONTHLY - Tiền thuê hàng tháng ===
        {
            "question": "Tổng tiền thuê hàng tháng",
            "sql": "SELECT SUM(amount) AS tong_tien_thue FROM payments WHERE payment_type = 'MONTHLY';"
        },
        {
            "question": "Tiền thuê đã thu",
            "sql": "SELECT SUM(amount) AS tong_tien_thue_da_thu FROM payments WHERE payment_type = 'MONTHLY' AND status = 1;"
        },
        {
            "question": "Tiền thuê theo tháng",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, SUM(amount) AS tong_tien_thue, COUNT(*) AS so_hop_dong
FROM payments
WHERE payment_type = 'MONTHLY' AND status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        {
            "question": "Tiền thuê chưa thanh toán",
            "sql": """SELECT c.contract_number, prop.title AS bds, p.amount, p.due_date
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
JOIN properties prop ON c.property_id = prop.property_id
WHERE p.payment_type = 'MONTHLY' AND p.status = 0
ORDER BY p.due_date;"""
        },
        {
            "question": "Tiền thuê quá hạn",
            "sql": """SELECT c.contract_number, prop.title AS bds, p.amount, p.due_date, 
       CURRENT_DATE - p.due_date AS so_ngay_qua_han
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
JOIN properties prop ON c.property_id = prop.property_id
WHERE p.payment_type = 'MONTHLY' AND p.status = 0 AND p.due_date < CURRENT_DATE
ORDER BY so_ngay_qua_han DESC;"""
        },
        
        # === PENALTY - Tiền phạt vi phạm hợp đồng ===
        {
            "question": "Tổng tiền phạt",
            "sql": "SELECT SUM(amount) AS tong_tien_phat FROM payments WHERE payment_type = 'PENALTY';"
        },
        {
            "question": "Tiền phạt đã thu",
            "sql": "SELECT SUM(amount) AS tong_tien_phat_da_thu FROM payments WHERE payment_type = 'PENALTY' AND status = 1;"
        },
        {
            "question": "Chi tiết các khoản phạt",
            "sql": """SELECT c.contract_number, c.contract_type, p.amount AS tien_phat, p.notes AS ly_do, p.paid_date, p.status
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'PENALTY'
ORDER BY p.created_at DESC;"""
        },
        {
            "question": "Tiền phạt chưa thu",
            "sql": """SELECT c.contract_number, p.amount AS tien_phat, p.notes AS ly_do, p.due_date
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'PENALTY' AND p.status = 0
ORDER BY p.due_date;"""
        },
        
        # === REFUND - Tiền hoàn trả ===
        {
            "question": "Tổng tiền hoàn trả",
            "sql": "SELECT SUM(amount) AS tong_hoan_tra FROM payments WHERE payment_type = 'REFUND';"
        },
        {
            "question": "Tiền hoàn trả đã thực hiện",
            "sql": "SELECT SUM(amount) AS tong_hoan_tra_da_tra FROM payments WHERE payment_type = 'REFUND' AND status = 1;"
        },
        {
            "question": "Chi tiết các khoản hoàn trả",
            "sql": """SELECT c.contract_number, c.contract_type, p.amount AS tien_hoan_tra, p.notes AS ly_do, p.paid_date, p.status
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'REFUND'
ORDER BY p.created_at DESC;"""
        },
        {
            "question": "Hoàn trả chờ xử lý",
            "sql": """SELECT c.contract_number, p.amount AS tien_hoan_tra, p.notes AS ly_do
FROM payments p
JOIN contract c ON p.contract_id = c.contract_id
WHERE p.payment_type = 'REFUND' AND p.status = 0
ORDER BY p.created_at DESC;"""
        },
        
        # === MONEY_SALE - Tiền chủ nhà nhận được từ việc bán BĐS ===
        {
            "question": "Tổng tiền chủ nhà nhận từ bán BĐS",
            "sql": "SELECT SUM(amount) AS tong_tien_ban FROM payments WHERE payment_type = 'MONEY_SALE';"
        },
        {
            "question": "Tiền bán BĐS đã thanh toán cho chủ nhà",
            "sql": "SELECT SUM(amount) AS tong_da_tra FROM payments WHERE payment_type = 'MONEY_SALE' AND status = 1;"
        },
        {
            "question": "Tiền bán BĐS chưa thanh toán cho chủ nhà",
            "sql": "SELECT SUM(amount) AS tong_chua_tra FROM payments WHERE payment_type = 'MONEY_SALE' AND status = 0;"
        },
        {
            "question": "Chi tiết tiền bán BĐS cho từng chủ nhà",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, prop.title AS bds, 
       p.amount AS tien_nhan, p.paid_date, p.status
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'MONEY_SALE'
ORDER BY p.amount DESC;"""
        },
        {
            "question": "Chủ nhà nhận tiền bán nhiều nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, u.email,
       SUM(p.amount) AS tong_tien_nhan, COUNT(p.payment_id) AS so_giao_dich
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'MONEY_SALE' AND p.status = 1
GROUP BY po.owner_id, u.last_name, u.first_name, u.email
ORDER BY tong_tien_nhan DESC LIMIT 10;"""
        },
        {
            "question": "Tiền bán BĐS chờ thanh toán cho chủ nhà",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, prop.title AS bds, 
       p.amount AS tien_can_tra, p.due_date
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'MONEY_SALE' AND p.status = 0
ORDER BY p.due_date;"""
        },
        
        # === MONEY_RENTAL - Tiền chủ nhà nhận từ cho thuê BĐS ===
        {
            "question": "Tổng tiền chủ nhà nhận từ cho thuê",
            "sql": "SELECT SUM(amount) AS tong_tien_thue FROM payments WHERE payment_type = 'MONEY_RENTAL';"
        },
        {
            "question": "Tiền cho thuê đã thanh toán cho chủ nhà",
            "sql": "SELECT SUM(amount) AS tong_da_tra FROM payments WHERE payment_type = 'MONEY_RENTAL' AND status = 1;"
        },
        {
            "question": "Tiền cho thuê chưa thanh toán cho chủ nhà",
            "sql": "SELECT SUM(amount) AS tong_chua_tra FROM payments WHERE payment_type = 'MONEY_RENTAL' AND status = 0;"
        },
        {
            "question": "Chi tiết tiền cho thuê cho từng chủ nhà",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, prop.title AS bds, 
       p.amount AS tien_nhan, p.paid_date, p.status
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'MONEY_RENTAL'
ORDER BY p.paid_date DESC;"""
        },
        {
            "question": "Chủ nhà nhận tiền cho thuê nhiều nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, u.email,
       SUM(p.amount) AS tong_tien_nhan, COUNT(p.payment_id) AS so_thang
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'MONEY_RENTAL' AND p.status = 1
GROUP BY po.owner_id, u.last_name, u.first_name, u.email
ORDER BY tong_tien_nhan DESC LIMIT 10;"""
        },
        {
            "question": "Tiền cho thuê theo tháng cho chủ nhà",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, SUM(amount) AS tong_tien_thue, COUNT(*) AS so_giao_dich
FROM payments
WHERE payment_type = 'MONEY_RENTAL' AND status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        
        # === SERVICE_FEE - Phí dịch vụ chủ nhà trả cho hệ thống ===
        {
            "question": "Tổng phí dịch vụ",
            "sql": "SELECT SUM(amount) AS tong_phi_dv FROM payments WHERE payment_type = 'SERVICE_FEE';"
        },
        {
            "question": "Phí dịch vụ đã thu",
            "sql": "SELECT SUM(amount) AS tong_phi_dv_da_thu FROM payments WHERE payment_type = 'SERVICE_FEE' AND status = 1;"
        },
        {
            "question": "Phí dịch vụ chưa thu",
            "sql": "SELECT SUM(amount) AS tong_phi_dv_chua_thu FROM payments WHERE payment_type = 'SERVICE_FEE' AND status = 0;"
        },
        {
            "question": "Chi tiết phí dịch vụ theo BĐS",
            "sql": """SELECT prop.title AS bds, CONCAT(u.last_name, ' ', u.first_name) AS owner_name,
       p.amount AS phi_dich_vu, p.paid_date, p.status
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'SERVICE_FEE'
ORDER BY p.created_at DESC;"""
        },
        {
            "question": "Phí dịch vụ theo tháng",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, SUM(amount) AS tong_phi_dv, COUNT(*) AS so_giao_dich
FROM payments
WHERE payment_type = 'SERVICE_FEE' AND status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        {
            "question": "Chủ nhà có phí dịch vụ cao nhất",
            "sql": """SELECT CONCAT(u.last_name, ' ', u.first_name) AS owner_name, u.email,
       SUM(p.amount) AS tong_phi_dv, COUNT(p.payment_id) AS so_bds
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'SERVICE_FEE' AND p.status = 1
GROUP BY po.owner_id, u.last_name, u.first_name, u.email
ORDER BY tong_phi_dv DESC LIMIT 10;"""
        },
        {
            "question": "Phí dịch vụ chờ thanh toán",
            "sql": """SELECT prop.title AS bds, CONCAT(u.last_name, ' ', u.first_name) AS owner_name,
       p.amount AS phi_dich_vu, p.due_date
FROM payments p
JOIN properties prop ON p.property_id = prop.property_id
JOIN property_owners po ON prop.owner_id = po.owner_id
JOIN users u ON po.owner_id = u.user_id
WHERE p.payment_type = 'SERVICE_FEE' AND p.status = 0
ORDER BY p.due_date;"""
        },
        
        # === TỔNG HỢP PAYMENT TYPES ===
        {
            "question": "Tổng hợp tất cả loại thanh toán",
            "sql": """SELECT 
    payment_type,
    CASE payment_type
        WHEN 'SALARY' THEN 'Tiền lương nhân viên'
        WHEN 'DEPOSIT' THEN 'Tiền đặt cọc'
        WHEN 'ADVANCE' THEN 'Tiền tạm ứng'
        WHEN 'INSTALLMENT' THEN 'Tiền trả góp'
        WHEN 'FULL_PAY' THEN 'Thanh toán một lần'
        WHEN 'MONTHLY' THEN 'Tiền thuê hàng tháng'
        WHEN 'PENALTY' THEN 'Tiền phạt'
        WHEN 'REFUND' THEN 'Tiền hoàn trả'
        WHEN 'MONEY_SALE' THEN 'Tiền bán BĐS (cho chủ nhà)'
        WHEN 'MONEY_RENTAL' THEN 'Tiền thuê (cho chủ nhà)'
        WHEN 'SERVICE_FEE' THEN 'Phí dịch vụ (chủ nhà trả)'
        ELSE payment_type
    END AS mo_ta,
    COUNT(*) AS so_giao_dich,
    SUM(amount) AS tong_tien,
    SUM(CASE WHEN status = 1 THEN amount ELSE 0 END) AS da_thanh_toan,
    SUM(CASE WHEN status = 0 THEN amount ELSE 0 END) AS chua_thanh_toan
FROM payments
GROUP BY payment_type
ORDER BY tong_tien DESC;"""
        },
        {
            "question": "Doanh thu hệ thống từ phí dịch vụ",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, 
       SUM(amount) AS doanh_thu_phi_dv
FROM payments
WHERE payment_type = 'SERVICE_FEE' AND status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        {
            "question": "Chi phí lương nhân viên theo tháng",
            "sql": """SELECT DATE_TRUNC('month', paid_date) AS thang, 
       SUM(amount) AS chi_phi_luong
FROM payments
WHERE payment_type = 'SALARY' AND status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        {
            "question": "Lợi nhuận ròng theo tháng",
            "sql": """SELECT 
    DATE_TRUNC('month', paid_date) AS thang,
    SUM(CASE WHEN payment_type = 'SERVICE_FEE' THEN amount ELSE 0 END) AS thu_phi_dv,
    SUM(CASE WHEN payment_type = 'SALARY' THEN amount ELSE 0 END) AS chi_luong,
    SUM(CASE WHEN payment_type = 'SERVICE_FEE' THEN amount ELSE 0 END) - 
    SUM(CASE WHEN payment_type = 'SALARY' THEN amount ELSE 0 END) AS loi_nhuan
FROM payments
WHERE status = 1 AND paid_date IS NOT NULL
GROUP BY DATE_TRUNC('month', paid_date)
ORDER BY thang DESC;"""
        },
        
        # ==================== LOCATIONS ====================
        {
            "question": "Danh sách thành phố",
            "sql": "SELECT city_name, description, total_area, avg_land_price, population, is_active FROM cities ORDER BY city_name;"
        },
        {
            "question": "BĐS theo thành phố",
            "sql": """SELECT c.city_name, COUNT(p.property_id) AS so_bds, AVG(p.price_amount) AS gia_trung_binh
FROM properties p
JOIN wards w ON p.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities c ON d.city_id = c.city_id
GROUP BY c.city_id, c.city_name ORDER BY so_bds DESC;"""
        },
        {
            "question": "BĐS theo quận huyện",
            "sql": """SELECT d.district_name, c.city_name, COUNT(p.property_id) AS so_bds, AVG(p.price_amount) AS gia_trung_binh
FROM properties p
JOIN wards w ON p.ward_id = w.ward_id
JOIN districts d ON w.district_id = d.district_id
JOIN cities c ON d.city_id = c.city_id
GROUP BY d.district_id, d.district_name, c.city_name ORDER BY so_bds DESC LIMIT 20;"""
        },
        {
            "question": "Giá đất trung bình theo quận",
            "sql": "SELECT district_name, avg_land_price, total_area, population FROM districts WHERE avg_land_price IS NOT NULL ORDER BY avg_land_price DESC;"
        },
        {
            "question": "Có bao nhiêu phường xã?",
            "sql": "SELECT COUNT(*) AS total_wards FROM wards;"
        },
        
        # ==================== NOTIFICATIONS ====================
        {
            "question": "Có bao nhiêu thông báo?",
            "sql": "SELECT COUNT(*) AS total_notifications FROM notifications;"
        },
        {
            "question": "Thông báo theo loại",
            "sql": "SELECT type, COUNT(*) AS so_luong FROM notifications GROUP BY type ORDER BY so_luong DESC;"
        },
        {
            "question": "Thông báo chưa đọc",
            "sql": "SELECT COUNT(*) AS unread_count FROM notifications WHERE is_read = FALSE OR is_read IS NULL;"
        },
        {
            "question": "Trạng thái gửi thông báo",
            "sql": "SELECT delivery_status, COUNT(*) AS so_luong FROM notifications GROUP BY delivery_status;"
        },
        
        # ==================== VIOLATIONS ====================
        {
            "question": "Có bao nhiêu báo cáo vi phạm?",
            "sql": "SELECT COUNT(*) AS total_violations FROM violation_reports;"
        },
        {
            "question": "Báo cáo vi phạm theo loại",
            "sql": "SELECT violation_type, COUNT(*) AS so_luong FROM violation_reports GROUP BY violation_type ORDER BY so_luong DESC;"
        },
        {
            "question": "Báo cáo vi phạm theo trạng thái",
            "sql": "SELECT status, COUNT(*) AS so_luong FROM violation_reports GROUP BY status ORDER BY so_luong DESC;"
        },
        {
            "question": "Vi phạm chưa xử lý",
            "sql": "SELECT violation_type, description, created_at FROM violation_reports WHERE status IN ('PENDING', 'REPORTED', 'UNDER_REVIEW') ORDER BY created_at DESC;"
        },
        
        # ==================== DOCUMENTS ====================
        {
            "question": "Có bao nhiêu loại giấy tờ?",
            "sql": "SELECT COUNT(*) AS total_document_types FROM document_types;"
        },
        {
            "question": "Danh sách loại giấy tờ",
            "sql": "SELECT name, description, is_compulsory FROM document_types ORDER BY name;"
        },
        {
            "question": "Giấy tờ theo trạng thái xác minh",
            "sql": "SELECT verification_status, COUNT(*) AS so_luong FROM identification_documents GROUP BY verification_status;"
        },
        {
            "question": "Giấy tờ chờ xác minh",
            "sql": "SELECT document_name, document_number, issuing_authority, created_at FROM identification_documents WHERE verification_status = 0 ORDER BY created_at DESC;"
        },
        
        # ==================== MEDIA ====================
        {
            "question": "Có bao nhiêu ảnh/video?",
            "sql": "SELECT COUNT(*) AS total_media FROM media;"
        },
        {
            "question": "Media theo loại",
            "sql": "SELECT media_type, COUNT(*) AS so_luong FROM media GROUP BY media_type ORDER BY so_luong DESC;"
        },
        {
            "question": "BĐS có nhiều ảnh nhất",
            "sql": """SELECT p.title, COUNT(m.media_id) AS so_anh
FROM properties p
LEFT JOIN media m ON m.property_id = p.property_id
GROUP BY p.property_id, p.title
ORDER BY so_anh DESC LIMIT 10;"""
        },
        
        # ==================== COMPLEX QUERIES ====================
        {
            "question": "Tổng quan hệ thống",
            "sql": """SELECT 
    (SELECT COUNT(*) FROM properties) AS total_properties,
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM customers) AS total_customers,
    (SELECT COUNT(*) FROM sale_agents) AS total_agents,
    (SELECT COUNT(*) FROM property_owners) AS total_owners,
    (SELECT COUNT(*) FROM contract) AS total_contracts,
    (SELECT COUNT(*) FROM appointment) AS total_appointments,
    (SELECT SUM(amount) FROM payments WHERE status = 1) AS total_revenue;"""
        },
        {
            "question": "Hiệu suất nhân viên tháng này",
            "sql": """SELECT 
    CONCAT(u.last_name, ' ', u.first_name) AS agent_name,
    sa.employee_code,
    COUNT(DISTINCT a.appointment_id) AS appointments_count,
    COUNT(DISTINCT c.contract_id) AS contracts_count,
    COALESCE(SUM(c.total_contract_amount), 0) AS contract_value
FROM sale_agents sa
JOIN users u ON sa.sale_agent_id = u.user_id
LEFT JOIN appointment a ON a.agent_id = sa.sale_agent_id 
    AND DATE_TRUNC('month', a.created_at) = DATE_TRUNC('month', CURRENT_DATE)
LEFT JOIN contract c ON c.agent_id = sa.sale_agent_id 
    AND DATE_TRUNC('month', c.created_at) = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY sa.sale_agent_id, u.last_name, u.first_name, sa.employee_code
ORDER BY contracts_count DESC, appointments_count DESC;"""
        },
        {
            "question": "Phân tích BĐS theo khoảng giá",
            "sql": """SELECT 
    CASE 
        WHEN price_amount < 1000000000 THEN 'Dưới 1 tỷ'
        WHEN price_amount < 3000000000 THEN '1-3 tỷ'
        WHEN price_amount < 5000000000 THEN '3-5 tỷ'
        WHEN price_amount < 10000000000 THEN '5-10 tỷ'
        ELSE 'Trên 10 tỷ'
    END AS khoang_gia,
    COUNT(*) AS so_bds,
    AVG(area) AS dien_tich_tb
FROM properties
GROUP BY khoang_gia
ORDER BY MIN(price_amount);"""
        }
    ]
    
    print("📚 Đang pre-populate agent memory cho Real Estate System...")
    for item in training_data:
        await agent_memory.save_tool_usage(
            question=item["question"],
            tool_name="run_sql",
            args={"sql": item["sql"]},
            context=mock_context,
            success=True,
            metadata={"source": "pre_training"}
        )
    print(f"✅ Đã thêm {len(training_data)} patterns vào memory!")

# ============================================================================
# Server Setup
# ============================================================================
server = VannaFastAPIServer(agent)

if __name__ == "__main__":
    print("🏠 Starting Vanna AI - Real Estate System Analysis...")
    print("📍 Access at: http://localhost:8000")
    print("\n" + "="*60)
    print("💡 CÁCH SỬ DỤNG:")
    print("="*60)
    print("1. Mở browser tại http://localhost:8000")
    print("2. Đặt câu hỏi tiếng Việt về dữ liệu bất động sản")
    print("3. Ví dụ câu hỏi:")
    print("   - 'Có bao nhiêu BĐS đang bán?'")
    print("   - 'Top 10 BĐS giá cao nhất'")
    print("   - 'Doanh thu tháng này'")
    print("   - 'Nhân viên nào có rating cao nhất?'")
    print("   - 'Thống kê BĐS theo thành phố'")
    print("\n📊 Kết nối cùng database PostgreSQL với Backend!")
    print("🧠 Agent Memory tự động học từ các query thành công!\n")
    
    # Pre-populate memory before starting server
    import asyncio
    asyncio.run(populate_memory())
    
    server.run()
