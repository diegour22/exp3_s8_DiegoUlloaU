/* ============================================================
   PRY2206 - Exp3 Semana 8
   Esquema: S8
   ============================================================ */
/* ============================================================
   CASO 1
   ============================================================ */
-- Trigger TRG_TOTAL_CONSUMOS
-- (script completo del trigger validado y funcionando)

CREATE OR REPLACE TRIGGER trg_total_consumos
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
DECLARE
  v_id_huesped_old  total_consumos.id_huesped%TYPE;
  v_id_huesped_new  total_consumos.id_huesped%TYPE;
  v_delta           NUMBER;
  v_id_error        reg_errores.id_error%TYPE;
  v_msg_error       reg_errores.msg_error%TYPE;
BEGIN
  /* ============================
     INSERT
     ============================ */
  IF INSERTING THEN
    v_id_huesped_new := :NEW.id_huesped;
    v_delta := :NEW.monto;

    MERGE INTO total_consumos tc
    USING (SELECT v_id_huesped_new AS id_huesped, v_delta AS delta FROM dual) x
    ON (tc.id_huesped = x.id_huesped)
    WHEN MATCHED THEN
      UPDATE SET tc.monto_consumos = tc.monto_consumos + x.delta
    WHEN NOT MATCHED THEN
      INSERT (id_huesped, monto_consumos)
      VALUES (x.id_huesped, x.delta);

  /* ============================
     DELETE
     ============================ */
  ELSIF DELETING THEN
    v_id_huesped_old := :OLD.id_huesped;
    v_delta := :OLD.monto;

    UPDATE total_consumos
    SET monto_consumos = monto_consumos - v_delta
    WHERE id_huesped = v_id_huesped_old;

    DELETE FROM total_consumos
    WHERE id_huesped = v_id_huesped_old
      AND monto_consumos <= 0;

  /* ============================
     UPDATE
     ============================ */
  ELSIF UPDATING THEN
    v_id_huesped_old := :OLD.id_huesped;
    v_id_huesped_new := :NEW.id_huesped;

    IF v_id_huesped_old = v_id_huesped_new THEN
      v_delta := :NEW.monto - :OLD.monto;

      UPDATE total_consumos
      SET monto_consumos = monto_consumos + v_delta
      WHERE id_huesped = v_id_huesped_new;

    ELSE
      UPDATE total_consumos
      SET monto_consumos = monto_consumos - :OLD.monto
      WHERE id_huesped = v_id_huesped_old;

      DELETE FROM total_consumos
      WHERE id_huesped = v_id_huesped_old
        AND monto_consumos <= 0;

      MERGE INTO total_consumos tc
      USING (SELECT v_id_huesped_new AS id_huesped, :NEW.monto AS delta FROM dual) x
      ON (tc.id_huesped = x.id_huesped)
      WHEN MATCHED THEN
        UPDATE SET tc.monto_consumos = tc.monto_consumos + x.delta
      WHEN NOT MATCHED THEN
        INSERT (id_huesped, monto_consumos)
        VALUES (x.id_huesped, x.delta);
    END IF;
  END IF;

EXCEPTION
  WHEN OTHERS THEN
    BEGIN
      v_msg_error := SQLERRM;

      SELECT NVL(MAX(id_error), 0) + 1
      INTO v_id_error
      FROM reg_errores;

      INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
      VALUES (v_id_error, 'TRG_TOTAL_CONSUMOS', v_msg_error);
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;

    RAISE;
END;
/





/* ============================================================
   CASO 2
   ============================================================ */
-- Secuencia SQ_ERROR
-- Package PKG_TOURS
-- Funciones FN_AGENCIA y FN_CONSUMOS_USD
-- Procedimiento SP_COBRANZA_DIARIA
-- Bloque de ejecución
ALTER SESSION DISABLE PARALLEL DML;
ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY';
/

/* ============================================================
   0) SECUENCIA PARA ERRORES (si ya existe, se ignora)
   ============================================================ */
BEGIN
  EXECUTE IMMEDIATE 'CREATE SEQUENCE SQ_ERROR START WITH 1 INCREMENT BY 1';
EXCEPTION
  WHEN OTHERS THEN
    -- si ya existe (ORA-00955) no hacemos nada
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/

/* ============================================================
   1) PACKAGE: PKG_TOURS
      - Función: monto USD por tours del huésped
      - Si no tiene tours, devuelve 0
   ============================================================ */
CREATE OR REPLACE PACKAGE PKG_TOURS IS
  FUNCTION fn_monto_tours_usd(p_id_huesped IN NUMBER) RETURN NUMBER;
  v_monto_tours NUMBER; -- optativo
END PKG_TOURS;
/

CREATE OR REPLACE PACKAGE BODY PKG_TOURS IS
  FUNCTION fn_monto_tours_usd(p_id_huesped IN NUMBER) RETURN NUMBER IS
    v_total NUMBER := 0;
  BEGIN
    SELECT NVL(SUM(t.valor_tour * NVL(ht.num_personas, 1)), 0)
      INTO v_total
      FROM huesped_tour ht
      JOIN tour t
        ON t.id_tour = ht.id_tour
     WHERE ht.id_huesped = p_id_huesped;

    v_monto_tours := v_total;
    RETURN v_total;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_monto_tours := 0;
      RETURN 0;
    WHEN OTHERS THEN
      v_monto_tours := 0;
      RETURN 0;
  END fn_monto_tours_usd;
END PKG_TOURS;
/

/* ============================================================
   2) FUNCIÓN: FN_AGENCIA (con log de errores SIN ORA-12838)
      - Retorna nombre de la agencia del huésped
      - Si NO_DATA_FOUND u otro error:
        inserta en REG_ERRORES usando SQ_ERROR y retorna
        "NO REGISTRA AGENCIA"
   ============================================================ */
CREATE OR REPLACE FUNCTION FN_AGENCIA(p_id_huesped IN NUMBER)
  RETURN VARCHAR2
IS
  PRAGMA AUTONOMOUS_TRANSACTION; -- << EVITA ORA-12838 al insertar en REG_ERRORES

  v_nom_agencia agencia.nom_agencia%TYPE;
  v_msg         VARCHAR2(300);
BEGIN
  SELECT TRIM(a.nom_agencia)
    INTO v_nom_agencia
    FROM huesped h
    JOIN agencia a
      ON a.id_agencia = h.id_agencia
   WHERE h.id_huesped = p_id_huesped;

  RETURN v_nom_agencia;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    v_msg := 'NO_DATA_FOUND para id_huesped=' || p_id_huesped;

    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
    VALUES (sq_error.NEXTVAL, 'FN_AGENCIA', v_msg);

    COMMIT; -- commit solo del log (transacción autónoma)
    RETURN 'NO REGISTRA AGENCIA';

  WHEN OTHERS THEN
    v_msg := SQLERRM;

    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
    VALUES (sq_error.NEXTVAL, 'FN_AGENCIA', v_msg);

    COMMIT; -- commit solo del log (transacción autónoma)
    RETURN 'NO REGISTRA AGENCIA';
END FN_AGENCIA;
/

/* ============================================================
   3) FUNCIÓN: FN_CONSUMOS_USD
      - Retorna consumos USD desde TOTAL_CONSUMOS
      - Si no registra consumos, devuelve 0
   ============================================================ */
CREATE OR REPLACE FUNCTION FN_CONSUMOS_USD(p_id_huesped IN NUMBER)
  RETURN NUMBER
IS
  v_total NUMBER := 0;
BEGIN
  SELECT NVL(tc.monto_consumos, 0)
    INTO v_total
    FROM total_consumos tc
   WHERE tc.id_huesped = p_id_huesped;

  RETURN v_total;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    RETURN 0;
END FN_CONSUMOS_USD;
/

/* ============================================================
   4) PROCEDIMIENTO: SP_COBRANZA_DIARIA
      - Procesa huéspedes con salida = fecha proceso
      - Limpia DETALLE_DIARIO_HUESPEDES y REG_ERRORES (TRUNCATE)
      - Calcula montos y descuentos en USD y guarda en CLP
      - Tipo de cambio (p_tipo_cambio) y fecha (p_fecha_proceso)
   ============================================================ */
CREATE OR REPLACE PROCEDURE SP_COBRANZA_DIARIA(
  p_fecha_proceso IN DATE,
  p_tipo_cambio   IN NUMBER
)
IS
  c_valor_persona_clp CONSTANT NUMBER := 35000;

  v_agencia              VARCHAR2(60);
  v_aloj_usd             NUMBER := 0;
  v_consumos_usd         NUMBER := 0;
  v_tours_usd            NUMBER := 0;

  v_personas             NUMBER := 1; -- no hay tabla explícita, se asume 1
  v_valor_personas_usd   NUMBER := 0;

  v_subtotal_usd         NUMBER := 0;

  v_pct_desc_consumos    NUMBER := 0;
  v_desc_consumos_usd    NUMBER := 0;

  v_desc_agencia_usd     NUMBER := 0;
  v_total_usd            NUMBER := 0;

  v_nombre_huesped       VARCHAR2(200);

BEGIN
  /* Limpieza para permitir ejecución repetida */
  EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_diario_huespedes';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE reg_errores';

  /* Huéspedes con salida = ingreso + estadia = fecha_proceso */
  FOR r IN (
    SELECT DISTINCT id_huesped
      FROM reserva
     WHERE TRUNC(ingreso + estadia) = TRUNC(p_fecha_proceso)
  ) LOOP

    /* Nombre completo */
    SELECT (h.nom_huesped || ' ' || h.appat_huesped || ' ' || h.apmat_huesped)
      INTO v_nombre_huesped
      FROM huesped h
     WHERE h.id_huesped = r.id_huesped;

    /* Agencia (con control de error y log) */
    v_agencia := FN_AGENCIA(r.id_huesped);

    /* Consumos USD */
    v_consumos_usd := FN_CONSUMOS_USD(r.id_huesped);

    /* Tours USD */
    v_tours_usd := PKG_TOURS.fn_monto_tours_usd(r.id_huesped);

    /* Alojamiento USD:
       (valor_habitacion + valor_minibar) * estadia
    */
    SELECT NVL(SUM((hbt.valor_habitacion + hbt.valor_minibar) * re.estadia), 0)
      INTO v_aloj_usd
      FROM reserva re
      JOIN detalle_reserva dr
        ON dr.id_reserva = re.id_reserva
      JOIN habitacion hbt
        ON hbt.id_habitacion = dr.id_habitacion
     WHERE re.id_huesped = r.id_huesped
       AND TRUNC(re.ingreso + re.estadia) = TRUNC(p_fecha_proceso);

    /* Cargo por persona:
       35.000 CLP -> convertir a USD para sumarlo al subtotal
    */
    v_valor_personas_usd := (c_valor_persona_clp / p_tipo_cambio) * v_personas;

    /* Subtotal USD */
    v_subtotal_usd := v_aloj_usd + v_consumos_usd + v_tours_usd + v_valor_personas_usd;

    /* % Descuento consumos según TRAMOS_CONSUMOS */
    BEGIN
      SELECT NVL(pct, 0)
        INTO v_pct_desc_consumos
        FROM tramos_consumos
       WHERE v_consumos_usd BETWEEN NVL(vmin_tramo, 0) AND NVL(vmax_tramo, 999999);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        v_pct_desc_consumos := 0;
      WHEN TOO_MANY_ROWS THEN
        SELECT NVL(pct, 0)
          INTO v_pct_desc_consumos
          FROM (
            SELECT pct
              FROM tramos_consumos
             WHERE v_consumos_usd BETWEEN NVL(vmin_tramo, 0) AND NVL(vmax_tramo, 999999)
             ORDER BY NVL(vmin_tramo,0) DESC
          )
         WHERE ROWNUM = 1;
    END;

    v_desc_consumos_usd := v_consumos_usd * v_pct_desc_consumos;

    /* Descuento agencia: 12% si agencia = Viajes Alberti */
    IF UPPER(TRIM(v_agencia)) = UPPER('Viajes Alberti') THEN
      v_desc_agencia_usd := v_subtotal_usd * 0.12;
    ELSE
      v_desc_agencia_usd := 0;
    END IF;

    /* Total USD */
    v_total_usd := v_subtotal_usd - v_desc_consumos_usd - v_desc_agencia_usd;

    /* Insert final en CLP (redondeado a entero) */
    INSERT INTO detalle_diario_huespedes(
      id_huesped,
      nombre,
      agencia,
      alojamiento,
      consumos,
      tours,
      subtotal_pago,
      descuento_consumos,
      descuentos_agencia,
      total
    )
    VALUES (
      r.id_huesped,
      v_nombre_huesped,
      v_agencia,
      ROUND(v_aloj_usd * p_tipo_cambio),
      ROUND(v_consumos_usd * p_tipo_cambio),
      ROUND(v_tours_usd * p_tipo_cambio),
      ROUND(v_subtotal_usd * p_tipo_cambio),
      ROUND(v_desc_consumos_usd * p_tipo_cambio),
      ROUND(v_desc_agencia_usd * p_tipo_cambio),
      ROUND(v_total_usd * p_tipo_cambio)
    );

  END LOOP;

  COMMIT;
END SP_COBRANZA_DIARIA;
/

/* ============================================================
   5) EJECUCIÓN (día del enunciado)
   ============================================================ */
BEGIN
  SP_COBRANZA_DIARIA(TO_DATE('18/08/2021','DD/MM/YYYY'), 915);
END;
/


