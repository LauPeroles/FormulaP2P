import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import datetime # Importamos datetime para los tipos
import traceback

print("Iniciando script de migración (Sintaxis v2.0)...")

try:
    # 1. Conectar a la Base de Datos
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("No se encontró la variable de entorno DATABASE_URL")
    
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    engine = create_engine(DATABASE_URL)
    
    # 2. Definir la estructura de la tabla (Sintaxis Moderna)
    class Base(DeclarativeBase):
        pass
    
    class P2PAnuncio(Base):
        __tablename__ = 'p2p_anuncios'
        
        # Columnas "Mapeadas" (sintaxis moderna)
        id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
        Timestamp: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
        Tipo: Mapped[str] = mapped_column(String(10), nullable=False)
        Precio: Mapped[float] = mapped_column(Float, nullable=False)
        Volumen: Mapped[float | None] = mapped_column(Float, nullable=True) # Permite nulos
        Metodos_Pago: Mapped[str | None] = mapped_column(String, nullable=True) # Permite nulos

    # 3. Crear la tabla
    print(f"Intentando conectar y crear la tabla '{P2PAnuncio.__tablename__}'...")
    Base.metadata.create_all(engine)
    
    print(f"¡Éxito! La tabla '{P2PAnuncio.__tablename__}' ha sido verificada/creada.")
    print("Migración completada.")

except Exception as e:
    print(f"❌ ERROR durante la migración: {e}")
    # Imprimimos el traceback completo para ver el error en detalle en los logs
    traceback.print_exc()
    exit(1) # Salimos con un código de error
