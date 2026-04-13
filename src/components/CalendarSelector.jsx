import React from 'react';
import Calendar from 'react-calendar';
import 'react-calendar/dist/Calendar.css';

export default function CalendarSelector({ value, onChange }) {
  return (
    <div>
      <Calendar onChange={onChange} value={value} />
    </div>
  );
}