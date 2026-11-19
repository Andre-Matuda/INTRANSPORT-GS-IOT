import json
import time
from getpass import getpass
from threading import Event

import oracledb
import paho.mqtt.client as mqtt

BROKER = "broker.hivemq.com"
PORTA = 1883
ID_PONTO = 1
TOPICO = f"smartbus/stops/{ID_PONTO}/incoming"

SQL_FROTA = """
    SELECT DISTINCT
        l.nm_linha,
        l.ds_linha,
        b.nr_placa
    FROM T_CT_OPERACAO o
    JOIN T_CT_LINHA l
        ON l.id_linha = o.T_CT_LINHA_id_linha
    JOIN T_CT_ONIBUS b
        ON b.id_onibus = o.T_CT_ONIBUS_id_onibus
    JOIN T_CT_LINHA_PONTO lp
        ON lp.T_CT_LINHA_id_linha = l.id_linha
    WHERE lp.T_CT_PONTO_ONIBUS_id_ponto = :id_ponto
      AND b.st_onibus = 'DISPONIVEL'
    ORDER BY l.nm_linha, b.nr_placa
"""


def buscar_frota():

    with oracledb.connect(
        user="SEU_USUARIO_ORACLE",
        password="SUA_SENHA_ORACLE",
        host="oracle.fiap.com.br",
        port=1521,
        sid="ORCL",
    ) as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(SQL_FROTA, id_ponto=ID_PONTO)
            return cursor.fetchall()


def main():
    frota = buscar_frota()
    if not frota:
        print(f"Nenhum ônibus disponível encontrado para o ponto {ID_PONTO}.")
        return

    print(f"{len(frota)} ônibus carregado(s) do Oracle para o ponto {ID_PONTO}.")
    print(f"Tópico MQTT: {TOPICO}")

    conectado = Event()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            print(f"O broker recusou a conexão: {reason_code}")
        else:
            conectado.set()

    client.on_connect = on_connect
    client.connect(BROKER, PORTA, keepalive=60)
    client.loop_start()

    try:
        if not conectado.wait(timeout=10):
            raise RuntimeError("O broker MQTT não confirmou a conexão.")

        while True:
            for linha, descricao, placa in frota:
                for eta in (5, 4, 3, 2, 1):
                    payload = {
                        "line": linha,
                        "busName": descricao,
                        "destination": descricao,
                        "vehicleId": placa,
                        "etaMinutes": eta,
                        "noService": False,
                        "source": "banco_oracle",
                    }

                    envio = client.publish(
                        TOPICO,
                        json.dumps(payload, ensure_ascii=False),
                        qos=1,
                        retain=False,
                    )
                    envio.wait_for_publish(timeout=10)
                    if not envio.is_published():
                        raise RuntimeError("O broker não confirmou a publicação.")

                    print(f"Publicado: {linha} | {descricao} | {eta} min")
                    # Dá tempo para a animação terminar antes do próximo ônibus.
                    time.sleep(7 if eta == 1 else 5)
    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTeste encerrado.")
    except (oracledb.Error, OSError, RuntimeError, ValueError) as erro:
        raise SystemExit(f"Erro: {erro}")
