const { DataTypes } = require("sequelize");

module.exports = (sequelize) => {
  const SovosGibUserList = sequelize.define(
    "SovosGibUserList",
    {
      id: {
        type: DataTypes.INTEGER,
        autoIncrement: true,
        primaryKey: true,
      },
      identifier: {
        type: DataTypes.STRING(20), 
        comment: "Kullanıcının VKN veya TCKN'si",
      },
      alias: {
        type: DataTypes.STRING(100), 
        allowNull: false,
        comment: "Kullanıcının GİB üzerindeki etiketi",
      },
      title: {
        type: DataTypes.STRING(100), 
        allowNull: true,
        comment: "Kullanıcının unvanı",
      },
      type: {
        type: DataTypes.ENUM("PK", "GB"), 
        allowNull: false,
      },
      document_type: {
        type: DataTypes.ENUM("Invoice", "DespatchAdvice"),
        allowNull: false,
        comment: "e-Fatura mı, e-İrsaliye mi?",
      },
      first_creation_time: {
        type: DataTypes.DATE,
        allowNull: false,
        comment: "GİB sistemine ilk kayıt tarihi",
      },
      is_active: {
        type: DataTypes.BOOLEAN,
        defaultValue: true,
        allowNull: false,
        comment: "Bu kayıt son senkronizasyonda GİB listesinde var mıydı?",
      },
      created_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      last_synced_at: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
    },
    {
      tableName: "sovos_gib_user_list",
      timestamps: false, 
      indexes: [
        {
          unique: true,
          fields: ["identifier", "alias", "document_type"],
          name: "unique_user_alias_doc_type",
        },
        {
          fields: ["identifier"],
        },
        {
          fields: ["is_active"],
        },
      ],
    }
  );

  return SovosGibUserList;
};
