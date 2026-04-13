/* cv-processor.js */

/**
 * Main entry point to process a CV after upload.
 * 1. Triggers backend to extract text from stored CV and analyze with Gemini.
 * 2. Merges Skills (adds to existing).
 * 3. Calculates Total Years of Experience (handling overlaps).
 * 4. Formats Experience and Education.
 * 5. Updates the Process table and the Namecard UI.
 */
async function processCV(linkedinUrl) {
    if (!linkedinUrl) return;

    // Helper to update status message in SourcingVerify
    const setStatus = (msg, type = 'info') => {
        const statusEl = document.getElementById('status');
        if (statusEl) {
            statusEl.textContent = msg;
            statusEl.className = type;
        }
    };

    setStatus('Analyzing CV with Gemini...', 'info');

    try {
        // 1. Call Backend to Extract & Analyze stored CV
        const analyzeRes = await fetch('/gemini/analyze_cv_stored', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ linkedinurl: linkedinUrl })
        });

        if (!analyzeRes.ok) {
            const err = await analyzeRes.json().catch(() => ({}));
            throw new Error(err.error || 'CV Analysis failed');
        }

        const data = await analyzeRes.json(); 
        // Expected data: { skills: [], experience_structured: [], education_structured: [] }

        // 2. Skillsets: Merge (Add new, do not replace existing)
        let currentSkills = [];
        try {
            // Fetch current skills from backend via geography endpoint if available
            if (window.getProcessGeography) {
                const geo = await window.getProcessGeography(linkedinUrl);
                if (geo && Array.isArray(geo.skillset)) currentSkills = geo.skillset;
            }
        } catch (e) { console.warn('Failed to fetch existing skills', e); }

        const newSkills = Array.isArray(data.skills) ? data.skills : [];
        // Deduplicate
        const mergedSkills = [...new Set([...currentSkills, ...newSkills])]; 

        // 3. Total Years of Experience Calculation (Handling Overlaps)
        const expStruct = Array.isArray(data.experience_structured) ? data.experience_structured : [];
        const now = new Date();
        const ranges = [];

        expStruct.forEach(role => {
            if (!role.start_year) return;

            // Parse Start
            const startYear = parseInt(role.start_year);
            const startMonth = role.start_month ? parseInt(role.start_month) - 1 : 0; // default Jan
            const startDate = new Date(startYear, startMonth);

            // Parse End
            let endDate = now;
            const endYearStr = String(role.end_year || '').toLowerCase();
            if (endYearStr === 'present' || !role.end_year) {
                endDate = now;
            } else {
                const endYear = parseInt(role.end_year);
                const endMonth = role.end_month ? parseInt(role.end_month) - 1 : 11; // default Dec
                endDate = new Date(endYear, endMonth);
            }

            if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime()) && endDate >= startDate) {
                ranges.push({ start: startDate, end: endDate });
            }
        });

        // Merge Overlapping Ranges
        ranges.sort((a, b) => a.start - b.start);
        const merged = [];
        for (let r of ranges) {
            if (!merged.length || merged[merged.length - 1].end < r.start) {
                merged.push(r);
            } else {
                // Merge overlapping: extend end date if needed
                merged[merged.length - 1].end = new Date(Math.max(merged[merged.length - 1].end, r.end));
            }
        }

        // Sum durations
        const totalMs = merged.reduce((acc, r) => acc + (r.end - r.start), 0);
        // Convert ms to years (approx)
        const totalYears = (totalMs / (1000 * 60 * 60 * 24 * 365.25)).toFixed(1);

        // 4a. Format Experience
        // Format: Job Title, Company Name, Start to End
        const experienceFormatted = expStruct.map(role => {
            const title = role.title || 'Unknown Role';
            const company = role.company || 'Unknown Company';
            const start = role.start_year || '?';
            const end = role.end_year || 'Present';
            return `${title}, ${company}, ${start} to ${end}`;
        }).join('\n');

        // 4b. Format Education
        // Format: University Name, Degree Type, Discipline
        const educationFormatted = (data.education_structured || []).map(edu => {
             const uni = edu.university || '';
             const deg = edu.degree || '';
             const disc = edu.discipline || '';
             return [uni, deg, disc].filter(s => s && s.trim()).join(', ');
        }).join('\n');

        // 5. Upload to Process Table
        setStatus('Saving extracted data to process table...', 'info');
        
        const updatePayload = {
            linkedinurl: linkedinUrl,
            fields: {
                skillset: mergedSkills.join(', '), // CSV string for storage
                exp: totalYears,                  // Mapped to 'exp' column
                experience: experienceFormatted,  // Replaces experience
                education: educationFormatted     // Replaces education
            }
        };

        const updateRes = await fetch('/process/update_fields_cv', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(updatePayload)
        });

        if (!updateRes.ok) {
            console.warn('Backend update failed', await updateRes.text());
        }

        // 6. Update UI (Namecard) immediately to reflect changes
        setStatus('CV Processed. Updating UI.', 'success');
        
        // Find the table row for this user
        const rows = document.querySelectorAll('#tableBody tr');
        let targetTr = null;
        for (let tr of rows) {
            if ((tr.dataset.linkedinurl === linkedinUrl) || (tr.dataset.anchor === linkedinUrl)) {
                targetTr = tr;
                break;
            }
        }

        if (targetTr && window.__sv_namecard) {
            // Update the namecard data
            let fullText = experienceFormatted;
            if (educationFormatted) {
                fullText += (fullText ? '\n\n' : '') + 'Education\n' + educationFormatted;
            }
            
            window.__sv_namecard.updateNameCard(targetTr, { 
                experience: fullText, 
                skillset: mergedSkills,
                exp_years: totalYears
            });
            
            // If the namecard is not open, toggle it open so user sees result
            // Check if next sibling is namecard row
            const next = targetTr.nextElementSibling;
            if (!next || !next.classList.contains('sv-namecard-row')) {
                window.__sv_namecard.toggleNameCard(targetTr);
            }
        }

    } catch (err) {
        console.error('ProcessCV Error:', err);
        setStatus('CV Analysis Error: ' + err.message, 'error');
    }
}

// Expose globally for SourcingVerify.html to use
window.processCV = processCV;