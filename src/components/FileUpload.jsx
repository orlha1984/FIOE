import React, { useState } from 'react';
import axios from 'axios';

export default function FileUpload({ onUpload }) {
  const [file, setFile] = useState(null);

  const handleChange = e => setFile(e.target.files[0]);
  const handleUpload = async () => {
    if (!file) return;
    const form = new FormData();
    form.append('file', file);
    await axios.post('/api/upload', form);
    onUpload && onUpload();
  };

  return (
    <div>
      <input type="file" accept=".xlsx" onChange={handleChange} />
      <button onClick={handleUpload}>Upload Excel</button>
    </div>
  );
}