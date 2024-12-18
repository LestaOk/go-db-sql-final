package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt))

	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// верните идентификатор последней добавленной записи
	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("SELECT * FROM parcel WHERE number = $1", number)
	// заполните объект Parcel данными из таблицы
	p := Parcel{}

	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return p, fmt.Errorf("parcel with number %d not found", number)
		}
		return p, fmt.Errorf("failed to get parcel from db: %w", err)
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	rows, err := s.db.Query("SELECT * FROM parcel WHERE client = $1", client)
	// здесь из таблицы может вернуться несколько строк
	// заполните срез Parcel данными из таблицы
	var res []Parcel

	if err != nil {
		return res, fmt.Errorf("failed to get parcels from db: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		p := Parcel{}
		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return res, fmt.Errorf("failed to get parcel from result response: %w", err)
		}
		res = append(res, p)
	}

	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("failed during iteration on elements: %w", err)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number))

	if err != nil {
		return fmt.Errorf("failed on parcel status update")
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	status, err := s.StatusByNumber(number)

	if err != nil {
		return fmt.Errorf("failed to get parcel for address update by number: %w", err)
	}

	if status != ParcelStatusRegistered {
		return fmt.Errorf("address update is not available for parcels in %s status", status)
	}

	_, err = s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number",
		sql.Named("address", address),
		sql.Named("number", number),
	)

	if err != nil {
		return fmt.Errorf("failed to update parcel address: %w", err)
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	status, err := s.StatusByNumber(number)
	if err != nil {
		return fmt.Errorf("failed to get parcel for deletion by number: %w", err)
	}

	if status != ParcelStatusRegistered {
		return fmt.Errorf("parcel delete is not available for parcels in %s status", status)
	}

	_, err = s.db.Exec("DELETE FROM parcel WHERE number = $1", number)

	if err != nil {
		return fmt.Errorf("failed to delete parcel: %w", err)
	}

	return nil
}

func (s ParcelStore) StatusByNumber(number int) (string, error) {
	var status string
	err := s.db.QueryRow("SELECT status FROM parcel WHERE number = $1", number).Scan(&status)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("parcel with number %d not found", number)
		}
		return "", fmt.Errorf("failed to query parcel status: %w", err)
	}

	return status, nil
}
