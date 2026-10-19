# 用户与个人建档服务

Spring Boot 服务，负责手机用户注册、读取和修改个人档案。数据写入 MySQL 表 `diet_health.user_profile`，不经过 Kafka，也不写饮食流水。

技术栈与现有 `meal-service` 相同：Spring Boot 4.1.1、Java 17、Spring MVC、Spring Data JPA、MySQL。端口是 `8010`，避免和 `meal-service` 的 `8080` 冲突。

## 流程

1. 手机调用 `POST /api/users/register`。
2. 服务校验账号、密码、年龄、身高、体重，以及性别、活动水平、目标。账号重复返回 `409`，字段无法识别返回 `400`。
3. 服务生成 16 位 `user_id`，计算 BMI，并用 PBKDF2-SHA256 保存密码哈希。明文密码不入库，接口也不返回哈希。
4. 向 `user_profile` 插入一行，`source_type` 固定为 `real`。
5. 按 Mifflin-St Jeor 计算 BMR，再乘活动系数得到 TDEE，连同 `user_id` 和 BMI 返回。
6. 手机保存 `user_id`。之后用它读取或修改档案；上传饮食时也带这个 ID。

活动系数：久坐 `1.200`，轻度 `1.375`，中度 `1.550`。

BMR：

- 男：`10×体重 + 6.25×身高 - 5×年龄 + 5`
- 女：`10×体重 + 6.25×身高 - 5×年龄 - 161`

`TDEE = BMR × 活动系数`。这两项不单独存表，每次由档案现算。

## 启动

```bash
cd /home/ubuntu/user-profile-service
mvn spring-boot:run
```

数据库连接在 `src/main/resources/application.properties`，使用本机 MySQL 的 `diet_health` 库。`ddl-auto=none`，不会改已有表结构。

## 接口

JSON 字段使用下划线命名，例如 `display_name`、`height_cm`。

### 注册

`POST /api/users/register`

```json
{
  "account": "zhangsan",
  "password": "secret123",
  "display_name": "张三",
  "age": 28,
  "gender": "男",
  "height_cm": 175,
  "weight_kg": 72,
  "waist_cm": 82,
  "activity_level": "轻度",
  "goal_type": "减重",
  "target_weight_kg": 68
}
```

`gender` 可传 `男`、`女`、`Male`、`Female`。`activity_level` 可传 `久坐`、`轻度`、`中度`。`goal_type` 可传 `减重`、`维持`、`增重`。腰围和目标体重可以不传；没传目标体重时，用当前体重。

成功返回 `201`。175cm、72kg、轻度活动时，BMI 为 `23.51`，BMR 为 `1678.75`，TDEE 为 `2308.28`。

### 读取档案

`GET /api/users/{user_id}`

只返回 `source_type=real` 且已有账号的用户。

### 修改档案

`PUT /api/users/{user_id}`

只提交要改的字段。服务会按新的身高、体重重算 BMI，并按新的活动水平重算活动系数。账号和 `user_id` 不能通过这个接口修改。

```json
{
  "weight_kg": 70,
  "activity_level": "中度"
}
```

体重改为 70kg、活动改为中度后，BMI 为 `22.86`，TDEE 为 `2571.06`。

## 代码结构

```text
src/main/java/com/example/userprofile/
├── UserProfileServiceApplication.java
├── UserProfileController.java    注册、读取、修改接口
├── ProfileService.java           校验和写库
├── UserProfile.java              对应 user_profile
├── BodyMetrics.java              BMI、BMR、TDEE
└── PasswordHasher.java           密码哈希
```

## 验证

`mvn test` 已通过。测试会真实连接 `diet_health`，覆盖非法性别、注册、重复账号、读取和修改，结束后删除测试账号。
