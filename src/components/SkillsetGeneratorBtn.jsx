import React from 'react';
import axios from 'axios';

export default function SkillsetGeneratorBtn({ onComplete }) {
  const handleClick = async () => {
    await axios.post('/api/generate-skillsets');
    onComplete && onComplete();
  };
  return <button onClick={handleClick}>Generate Skillsets</button>;
}