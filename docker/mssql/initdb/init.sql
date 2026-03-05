-- Initialize database and base tables for local development
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'data_warehouse')
BEGIN
    CREATE DATABASE [data_warehouse];
END
GO

USE [data_warehouse];
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sensor_logs')
BEGIN
    CREATE TABLE sensor_logs (
        id INT IDENTITY(1,1) PRIMARY KEY,
        sensor_id NVARCHAR(255),
        val NVARCHAR(255) NULL,
        unit NVARCHAR(50) NULL,
        ts DATETIME2 NULL
    );
END
GO
