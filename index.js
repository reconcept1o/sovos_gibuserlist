const { Sequelize, DataTypes } = require("sequelize");
const dotenv = require("dotenv");
const path = require("path");

// .env dosyasını yükle
dotenv.config({
  path: path.resolve(
    __dirname,
    `.env.${process.env.NODE_ENV || "development"}`
  ),
});

// Veritabanı bağlantısı
const sequelize = new Sequelize(
  process.env.DB_NAME,
  process.env.DB_USER,
  process.env.DB_PASSWORD,
  {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    dialect: "postgres",
  }
);

// Modeli yükle
const SovosGibUserList = require("./models/SovosGibUserList")(
  sequelize,
  DataTypes
);

// Test endpoint’i
async function checkUsers() {
  try {
    const users = await SovosGibUserList.findAll({
      where: { is_active: true },
      limit: 10,
    });
    console.log(users);
  } catch (error) {
    console.error("Hata:", error);
  } finally {
    await sequelize.close();
  }
}

checkUsers();
