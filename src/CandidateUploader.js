import React, { useState, useEffect } from 'react';
import * as XLSX from 'xlsx';

function CandidateUploader() {
  const [file, setFile] = useState(null);
  const [candidates, setCandidates] = useState([]);

  useEffect(() => {
    // Fetch current candidates from backend on mount
    fetch('http://localhost:4000/candidates')
      .then(res => res.json())
      .then(data => setCandidates(data));
  }, []);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  // Restored mapping for Project_Title and Project Date variants
  const mapRow = (row) => {
    const first = (...keys) => {
      for (const k of keys) {
        if (Object.prototype.hasOwnProperty.call(row, k) && row[k] != null && String(row[k]).trim() !== '') {
          return row[k];
        }
      }
      return undefined;
    };

    const pTitle = first('Project_Title', 'Project Title', 'project_title', 'project_name', 'Project Name') || '';
    const pDateRaw = first('Project_Date', 'Project Date', 'project_date', 'employment_date', 'Employment Date') ?? null;

    return {
      project_title: pTitle,
      project_date: pDateRaw,
      project_name: first('project_name', 'Project Name') ?? pTitle,
      employment_date: first('employment_date', 'Employment Date') ?? pDateRaw,

      name: first('Name', 'name') || '',
      role: first('Role', 'role') || '',
      organisation: first('Organisation', 'organisation') || '',
      sector: first('Sector', 'sector') || '',
      job_family: first('Job Family', 'job_family') || '',
      role_tag: first('Role Tag', 'role_tag') || '',
      skillset: first('Skillset', 'skillset') || '',
      geographic: first('Geographic', 'geographic') || '',
      country: first('Country', 'country') || '',
      email: first('Email', 'email') || '',
      mobile: first('Mobile', 'mobile') || '',
      office: first('Office', 'office') || '',
      personal: first('Personal', 'personal') || '',
      seniority: first('Seniority', 'seniority') || '',
      sourcing_status: first('Sourcing Status', 'sourcing_status') || ''
    };
  };

  const handleUpload = async () => {
    if (!file) return alert('Please select a file');

    let mapped = [];
    const ext = file.name.split('.').pop().toLowerCase();

    if (ext === 'csv') {
      const text = await file.text();
      const workbook = XLSX.read(text, { type: 'string' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const json = XLSX.utils.sheet_to_json(worksheet);
      mapped = json.map(mapRow);
    } else if (ext === 'xlsx' || ext === 'xls') {
      const data = await file.arrayBuffer();
      const workbook = XLSX.read(data);
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const json = XLSX.utils.sheet_to_json(worksheet);
      mapped = json.map(mapRow);
    } else {
      return alert('Unsupported file type. Please select CSV, XLSX, or XLS.');
    }

    const res = await fetch('http://localhost:4000/candidates/bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ candidates: mapped })
    });
    if (res.ok) {
      alert('Upload successful!');
      fetch('http://localhost:4000/candidates')
        .then(res => res.json())
        .then(data => setCandidates(data));
    } else {
      alert('Upload failed!');
    }
  };

  const handleDelete = async (id) => {
    const res = await fetch(`http://localhost:4000/candidates/${id}`, {
      method: 'DELETE'
    });
    if (res.ok) {
      setCandidates(candidates.filter(c => c.id !== id));
    } else {
      alert('Delete failed!');
    }
  };

  return (
    <div>
      <h2>Candidate Spreadsheet Upload</h2>
      <input type="file" accept=".xlsx, .xls, .csv" onChange={handleFileChange} />
      <button onClick={handleUpload}>Upload to Postgres</button>
      <h3>Candidate List</h3>
      <table border="1">
        <thead>
          <tr>
            <th>Name</th>
            <th>Role</th>
            <th>Organisation</th>
            <th>Sector</th>
            <th>Job Family</th>
            <th>Delete</th>
          </tr>
        </thead>
        <tbody>
          {candidates.map((c) => (
            <tr key={c.id}>
              <td>{c.name}</td>
              <td>{c.role}</td>
              <td>{c.organisation}</td>
              <td>{c.sector}</td>
              <td>{c.job_family}</td>
              <td>
                <button onClick={() => handleDelete(c.id)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default CandidateUploader;