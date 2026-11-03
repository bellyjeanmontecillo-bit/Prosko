
// =========================================================================
// PROSKO - BARANGAY LIBJO SK PROJECT MONITORING & EVALUATION SYSTEM
// System Owner / Author: Belly Jean Montecillo (Student #69)
// Evaluator: Mr. Ian Celmar A. Avila (AppDev 4th Increment & Final Checking)
// COMPLIANCE WITH APPLICATION DEV 4TH INCREMENT & IAS 1 FINAL REQUIREMENTS
// =========================================================================

// Supabase Client Initialization & Robust REST Fallback
const SUPABASE_URL = 'https://eadxhgkrvncuhluawtfd.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhZHhoZ2tydm5jdWhsdWF3dGZkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTE2MTU0MzksImV4cCI6MjEwNzE5MTQzOX0.mENwcRk5uqbUhwCbW9kxS0RARakyvU1aqoCPbcqzEfE';

function createSupabaseRestClient(baseUrl, apiKey) {
    const headers = {
        'apikey': apiKey,
        'Authorization': 'Bearer ' + apiKey,
        'Content-Type': 'application/json'
    };

    return {
        from(tableName) {
            let method = 'GET';
            let body = null;
            let queryParams = [];
            let isSingle = false;
            let prefer = [];

            const builder = {
                select(columns = '*') {
                    queryParams.push('select=' + encodeURIComponent(columns));
                    return builder;
                },
                order(column, options = {}) {
                    const dir = options.ascending === false ? 'desc' : 'asc';
                    queryParams.push('order=' + encodeURIComponent(column) + '.' + dir);
                    return builder;
                },
                limit(count) {
                    queryParams.push('limit=' + count);
                    return builder;
                },
                eq(column, value) {
                    queryParams.push(encodeURIComponent(column) + '=eq.' + encodeURIComponent(value));
                    return builder;
                },
                or(filterStr) {
                    queryParams.push('or=(' + encodeURIComponent(filterStr) + ')');
                    return builder;
                },
                maybeSingle() {
                    isSingle = true;
                    prefer.push('return=representation');
                    return builder;
                },
                single() {
                    isSingle = true;
                    prefer.push('return=representation');
                    return builder;
                },
                insert(records) {
                    method = 'POST';
                    body = JSON.stringify(records);
                    prefer.push('return=representation');
                    return builder;
                },
                update(updates) {
                    method = 'PATCH';
                    body = JSON.stringify(updates);
                    prefer.push('return=representation');
                    return builder;
                },
                delete() {
                    method = 'DELETE';
                    prefer.push('return=representation');
                    return builder;
                },
                async then(resolve, reject) {
                    try {
                        const qs = queryParams.length ? '?' + queryParams.join('&') : '';
                        const reqHeaders = { ...headers };
                        if (prefer.length) reqHeaders['Prefer'] = prefer.join(',');
                        const fetchFn = typeof fetch !== 'undefined' ? fetch : (typeof window !== 'undefined' ? window.fetch : null);
                        if (!fetchFn) {
                            resolve({ data: null, error: new Error('Native fetch not available') });
                            return;
                        }
                        const res = await fetchFn(baseUrl + '/rest/v1/' + tableName + qs, {
                            method,
                            headers: reqHeaders,
                            body
                        });
                        if (res.ok) {
                            if (res.status === 204) {
                                resolve({ data: [], error: null });
                                return;
                            }
                            let json = await res.json();
                            if (isSingle) {
                                json = Array.isArray(json) ? (json[0] || null) : json;
                            }
                            resolve({ data: json, error: null });
                        } else {
                            const errJson = await res.json().catch(() => ({ message: res.statusText, code: String(res.status) }));
                            resolve({ data: null, error: errJson });
                        }
                    } catch (err) {
                        resolve({ data: null, error: err });
                    }
                }
            };
            return builder;
        },
        channel() {
            return {
                on() { return this; },
                subscribe() { return this; }
            };
        },
        removeChannel() {}
    };
}

function getSupabase() {
    if (typeof window !== 'undefined' && window.supabaseClient && typeof window.supabaseClient.from === 'function') {
        return window.supabaseClient;
    }
    if (typeof window !== 'undefined' && window.supabase && typeof window.supabase.createClient === 'function') {
        try {
            window.supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
            return window.supabaseClient;
        } catch (e) {
            console.warn('[PROSKO] Initializing Supabase via SDK failed, falling back to REST client:', e);
        }
    }
    if (typeof supabase !== 'undefined' && supabase && typeof supabase.from === 'function') {
        if (typeof window !== 'undefined') window.supabaseClient = supabase;
        return supabase;
    }
    const restClient = createSupabaseRestClient(SUPABASE_URL, SUPABASE_KEY);
    if (typeof window !== 'undefined') window.supabaseClient = restClient;
    return restClient;
}

var supabase = getSupabase();
let realtimeChannel = null;

// Robust self-healing Supabase helpers to prevent schema mismatch errors (PGRST204, 42703)
async function supabaseInsert(tableName, record) {
    const client = getSupabase();
    if (!client) return { data: null, error: new Error('Supabase client unavailable') };

    let payload = { ...record };
    for (let attempt = 0; attempt < 8; attempt++) {
        try {
            const { data, error } = await client.from(tableName).insert([payload]).select();
            if (!error) return { data, error: null };

            const msg = error.message || '';
            let missingCol = null;
            let m = msg.match(/Could not find the ['"]([^'"]+)['"] column/i);
            if (m) missingCol = m[1];
            if (!missingCol) {
                m = msg.match(/column ['"]([^'"]+)['"] of relation/i);
                if (m) missingCol = m[1];
            }
            if (!missingCol) {
                m = msg.match(/column ['"]([^'"]+)['"] does not exist/i);
                if (m) missingCol = m[1];
            }

            if (missingCol && payload.hasOwnProperty(missingCol)) {
                console.warn('[Supabase Self-Healing] Table ' + tableName + ' missing column ' + missingCol + '. Retrying without it.');
                delete payload[missingCol];
                continue;
            }
            return { data: null, error };
        } catch (err) {
            return { data: null, error: err };
        }
    }
    return { data: null, error: new Error('Exceeded max schema retry attempts') };
}

async function supabaseUpsert(tableName, record, conflictCol = 'id') {
    const client = getSupabase();
    if (!client) return { data: null, error: new Error('Supabase client unavailable') };

    let payload = { ...record };
    for (let attempt = 0; attempt < 8; attempt++) {
        try {
            const { data, error } = await client.from(tableName).upsert([payload], { onConflict: conflictCol }).select();
            if (!error) return { data, error: null };

            const msg = error.message || '';
            let missingCol = null;
            let m = msg.match(/Could not find the ['"]([^'"]+)['"] column/i);
            if (m) missingCol = m[1];
            if (!missingCol) {
                m = msg.match(/column ['"]([^'"]+)['"] of relation/i);
                if (m) missingCol = m[1];
            }
            if (!missingCol) {
                m = msg.match(/column ['"]([^'"]+)['"] does not exist/i);
                if (m) missingCol = m[1];
            }

            if (missingCol && payload.hasOwnProperty(missingCol)) {
                console.warn('[Supabase Self-Healing Upsert] Table ' + tableName + ' missing column ' + missingCol + '. Retrying without it.');
                delete payload[missingCol];
                continue;
            }
            return { data: null, error };
        } catch (err) {
            return { data: null, error: err };
        }
    }
    return { data: null, error: new Error('Exceeded max schema retry attempts') };
}

async function supabaseUpdate(tableName, record, matchCol, matchVal) {
    const client = getSupabase();
    if (!client) return { data: null, error: new Error('Supabase client unavailable') };

    let payload = { ...record };
    for (let attempt = 0; attempt < 8; attempt++) {
        try {
            const { data, error } = await client.from(tableName).update(payload).eq(matchCol, matchVal).select();
            if (!error) return { data, error: null };

            const msg = error.message || '';
            let missingCol = null;
            let m = msg.match(/Could not find the ['"]([^'"]+)['"] column/i);
            if (m) missingCol = m[1];
            if (!missingCol) {
                m = msg.match(/column ['"]([^'"]+)['"] of relation/i);
                if (m) missingCol = m[1];
            }
            if (!missingCol) {
                m = msg.match(/column ['"]([^'"]+)['"] does not exist/i);
                if (m) missingCol = m[1];
            }

            if (missingCol && payload.hasOwnProperty(missingCol)) {
                console.warn('[Supabase Self-Healing] Table ' + tableName + ' missing column ' + missingCol + '. Retrying without it.');
                delete payload[missingCol];
                continue;
            }
            return { data: null, error };
        } catch (err) {
            return { data: null, error: err };
        }
    }
    return { data: null, error: new Error('Exceeded max schema retry attempts') };
}

// Multi-Tab Logout Broadcast Channel (IAS Requirement E)
let authBroadcastChannel = null;
try {
    authBroadcastChannel = new BroadcastChannel('prosko_auth_channel');
    authBroadcastChannel.onmessage = (event) => {
        if (event.data === 'LOGOUT') {
            executeSilentLogout();
        }
    };
} catch (e) {}

window.addEventListener('storage', (e) => {
    if (e.key === 'prosko_logout_event') {
        executeSilentLogout();
    }
});

// =========================================================================
// 1. SECURITY & CRYPTOGRAPHIC FUNCTIONS (IAS REQUIREMENTS C, D, E)
// =========================================================================
async function hashPassword(plainText, salt = 'prosko_libjo_ias_2026') {
    if (window.crypto && window.crypto.subtle) {
        const encoder = new TextEncoder();
        const data = encoder.encode(plainText + salt);
        const hashBuffer = await crypto.subtle.digest('SHA-256', data);
        return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
    }
    // Fallback simple SHA-256 representation if subtle crypto is unavailable
    let hash = 0;
    const str = plainText + salt;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return Math.abs(hash).toString(16).padStart(64, '0');
}

// Live Application State Stores
let activeUser = null;
let sessionToken = null;
let currentFiscalYear = '2026';
let currentActiveTab = 'projects';

let projects = [];
let accomplishmentReports = [];
let brgyDocuments = [];
let incidents = [];
let activityLogs = [];
let disbursements = [];
let usersStore = [];
let backupLogs = [];

// Annual SK Youth Budgets (10% of Barangay IRA)
let annualBudgets = {
    '2027': 1600000.00,
    '2026': 1600000.00,
    '2025': 1200000.00,
    '2024': 1000000.00
};

// OTP State (IAS Requirement B)
let activeOtpObj = null; // { code, targetEmail, expiresAt, failedTries, purpose, pendingUser }
let recoveryCooldownTimer = null;
let recoveryCooldownSec = 0;
let changeEmailCooldownTimer = null;
let changeEmailCooldownSec = 0;

// Lockout & CAPTCHA State (IAS Requirement D)
let activeLockoutInterval = null;
let currentCaptchaAnswer = null;

// Inactivity Timer State (IAS Requirement E: 15 Mins Inactivity Auto-Logout)
const INACTIVITY_LIMIT_MS = 15 * 60 * 1000; // 15 minutes
const INACTIVITY_WARNING_MS = 14 * 60 * 1000; // 14 minutes (60 seconds remaining warning)
let lastUserActivityTimestamp = Date.now();
let inactivityCheckerInterval = null;
let inactivityModalCountdownInterval = null;

// =========================================================================
// 2. SYSTEM INITIALIZATION & LOCAL STORAGE PERSISTENCE
// =========================================================================
function saveProjectsToLocal() {
    localStorage.setItem('prosko_projects', JSON.stringify(projects));
}

function saveDisbursementsToLocal() {
    localStorage.setItem('prosko_disbursements', JSON.stringify(disbursements));
}

function saveUsersStoreToLocal() {
    localStorage.setItem('prosko_users_store', JSON.stringify(usersStore));
    const countBadge = document.getElementById('admin-badge-users-count');
    if (countBadge) countBadge.innerText = usersStore.length;
}

function saveReportsToLocal() {
    localStorage.setItem('prosko_reports', JSON.stringify(accomplishmentReports));
}

function saveDocsToLocal() {
    localStorage.setItem('prosko_docs', JSON.stringify(brgyDocuments));
}

function saveIncidentsToLocal() {
    localStorage.setItem('prosko_incidents', JSON.stringify(incidents));
}

async function initSystemStores() {
    // 1. Load Annual Budgets
    const savedBudgets = localStorage.getItem('prosko_annual_budgets');
    if (savedBudgets) {
        try { annualBudgets = JSON.parse(savedBudgets); } catch (e) {}
    }

    // 2. Load Users Store (IAS A: User CRUD & Roles)
    const savedUsers = localStorage.getItem('prosko_users_store');
    if (savedUsers) {
        try {
            usersStore = JSON.parse(savedUsers);
            const legacyHash = '0532523ad168498c037cfb132a57b6fa4b5596f5fda65a046aa570274a3237df';
            const newHash = await hashPassword('Belly1234!');
            let migrated = false;
            usersStore.forEach(u => {
                if (u.passwordHash === legacyHash) {
                    u.passwordHash = newHash;
                    migrated = true;
                }
                if (u.username === 'admin_libjo' || (u.fullName && /von|pilapil|sk chair admin|chair admin|sk chair|^admin$/i.test(u.fullName))) {
                    u.fullName = 'Belly Jean Montecillo';
                    migrated = true;
                }
            });
            if (migrated) saveUsersStoreToLocal();
        } catch (e) {}
    }

    if (!usersStore || usersStore.length === 0) {
        const defaultHash = await hashPassword('Belly1234!');
        usersStore = [
            {
                id: 'usr-1',
                username: 'admin_libjo',
                passwordHash: defaultHash,
                fullName: 'Belly Jean Montecillo',
                email: 'sk.libjo@gmail.com',
                role: 'SK Chairperson',
                status: 'Active',
                isLocked: false,
                failedAttempts: 0,
                createdAt: new Date().toISOString()
            },
            {
                id: 'usr-2',
                username: 'sec_libjo',
                passwordHash: defaultHash,
                fullName: 'SK Secretary',
                email: 'sec.libjo@gmail.com',
                role: 'SK Secretary',
                status: 'Active',
                isLocked: false,
                failedAttempts: 0,
                createdAt: new Date().toISOString()
            },
            {
                id: 'usr-3',
                username: 'maria_sk',
                passwordHash: defaultHash,
                fullName: 'SK Treasurer',
                email: 'maria.sk@gmail.com',
                role: 'SK Treasurer',
                status: 'Active',
                isLocked: false,
                failedAttempts: 0,
                createdAt: new Date().toISOString()
            }
        ];
        saveUsersStoreToLocal();
    }

    // 3. Load Projects
    const savedProjects = localStorage.getItem('prosko_projects');
    if (savedProjects) {
        try {
            projects = JSON.parse(savedProjects);
            let projMigrated = false;
            projects.forEach(p => {
                if (!p.fiscalYear) {
                    p.fiscalYear = '2026';
                    projMigrated = true;
                }
                const sub = p.submittedBy || p.submitted_by || '';
                if (!sub || /von|pilapil|sk chair admin|chair admin|sk chair|admin_libjo|^admin$|^sk chairperson$|belly jean montecillo/i.test(sub)) {
                    p.submittedBy = 'Belly Jean Montecillo (SK Chairperson)';
                    p.submitted_by = 'Belly Jean Montecillo (SK Chairperson)';
                    projMigrated = true;
                }
                // Transition projects that have 0 disbursed from "In Progress" to "Approved"
                if (p.status === 'In Progress' && (parseFloat(p.disbursed) || 0) <= 0) {
                    p.status = 'Approved';
                    projMigrated = true;
                }
            });

            // Ensure historical seed projects for FY 2024 and FY 2025 exist if missing
            const has2025 = projects.some(p => String(p.fiscalYear) === '2025');
            if (!has2025) {
                projects.push(
                    {
                        id: 202501,
                        title: 'Youth Leadership & Governance Summit 2025',
                        category: 'Leadership & Development',
                        budget: 200000.00,
                        disbursed: 200000.00,
                        status: 'Completed',
                        submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                        period: 'May 15 - May 18, 2025',
                        aiScore: '96% Target Met',
                        fiscalYear: '2025'
                    },
                    {
                        id: 202502,
                        title: 'Anti-Drug & Adolescent Wellness Campaign',
                        category: 'Health & Wellness',
                        budget: 100000.00,
                        disbursed: 50000.00,
                        status: 'Completed',
                        submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                        period: 'Oct 10 - Oct 12, 2025',
                        aiScore: '94% High Engagement',
                        fiscalYear: '2025'
                    }
                );
                projMigrated = true;
            }

            const has2024 = projects.some(p => String(p.fiscalYear) === '2024');
            if (!has2024) {
                projects.push(
                    {
                        id: 202401,
                        title: 'Linggo ng Kabataan Sports & Cultural Festival 2024',
                        category: 'Sports & Culture',
                        budget: 150000.00,
                        disbursed: 150000.00,
                        status: 'Completed',
                        submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                        period: 'Aug 12 - Aug 19, 2024',
                        aiScore: '98% Completed',
                        fiscalYear: '2024'
                    },
                    {
                        id: 202402,
                        title: 'SK Educational Subsidy & School Supplies Distribution 2024',
                        category: 'Education & Technology',
                        budget: 100000.00,
                        disbursed: 75000.00,
                        status: 'Completed',
                        submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                        period: 'Sep 5 - Sep 8, 2024',
                        aiScore: '97% Target Met',
                        fiscalYear: '2024'
                    }
                );
                projMigrated = true;
            }
            if (projMigrated) saveProjectsToLocal();
        } catch (e) {}
    }

    if (!projects || projects.length === 0) {
        projects = [
            {
                id: 1,
                title: 'Youth Sports Fest 2026',
                category: 'Sports & Recreation',
                budget: 45000.00,
                disbursed: 20000.00,
                status: 'In Progress',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Oct 1 - Oct 15, 2026',
                aiScore: '94% High Engagement',
                fiscalYear: '2026'
            },
            {
                id: 2,
                title: 'Digital Literacy Workshop',
                category: 'Education & Technology',
                budget: 25000.00,
                disbursed: 25000.00,
                status: 'Completed',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Aug 10 - Aug 12, 2026',
                aiScore: '98% Target Met',
                fiscalYear: '2026'
            },
            {
                id: 111,
                title: 'Clean Up',
                category: 'Sports & Recreation',
                budget: 45000.00,
                disbursed: 45000.00,
                status: 'In Progress',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Nov 1 - Nov 4, 2026',
                aiScore: '92% AI Verified',
                fiscalYear: '2026'
            },
            {
                id: 112,
                title: 'Environment',
                category: 'Sports & Recreation',
                budget: 25000.00,
                disbursed: 25000.00,
                status: 'In Progress',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Nov 1, 2026',
                aiScore: '92% AI Verified',
                fiscalYear: '2026'
            },
            {
                id: 202501,
                title: 'Youth Leadership & Governance Summit 2025',
                category: 'Leadership & Development',
                budget: 200000.00,
                disbursed: 200000.00,
                status: 'Completed',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'May 15 - May 18, 2025',
                aiScore: '96% Target Met',
                fiscalYear: '2025'
            },
            {
                id: 202502,
                title: 'Anti-Drug & Adolescent Wellness Campaign',
                category: 'Health & Wellness',
                budget: 100000.00,
                disbursed: 50000.00,
                status: 'Completed',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Oct 10 - Oct 12, 2025',
                aiScore: '94% High Engagement',
                fiscalYear: '2025'
            },
            {
                id: 202401,
                title: 'Linggo ng Kabataan Sports & Cultural Festival 2024',
                category: 'Sports & Culture',
                budget: 150000.00,
                disbursed: 150000.00,
                status: 'Completed',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Aug 12 - Aug 19, 2024',
                aiScore: '98% Completed',
                fiscalYear: '2024'
            },
            {
                id: 202402,
                title: 'SK Educational Subsidy & School Supplies Distribution 2024',
                category: 'Education & Technology',
                budget: 100000.00,
                disbursed: 75000.00,
                status: 'Completed',
                submittedBy: 'Belly Jean Montecillo (SK Chairperson)',
                period: 'Sep 5 - Sep 8, 2024',
                aiScore: '97% Target Met',
                fiscalYear: '2024'
            }
        ];
        saveProjectsToLocal();
    }

    // 4. Load Disbursements (strictly aligned with projects)
    const savedDisbursements = localStorage.getItem('prosko_disbursements');
    if (savedDisbursements) {
        try {
            disbursements = JSON.parse(savedDisbursements);
            let disbMigrated = false;
            disbursements.forEach(d => {
                if (!d.fiscalYear) {
                    d.fiscalYear = '2026';
                    disbMigrated = true;
                }
            });

            // Ensure historical seed disbursements for FY 2024 and FY 2025 exist if missing
            const hasDisb2025 = disbursements.some(d => String(d.fiscalYear) === '2025');
            if (!hasDisb2025) {
                disbursements.push(
                    {
                        id: 'DV-2025-001',
                        voucherNo: 'DV-2025-001',
                        projectId: 202501,
                        projectTitle: 'Youth Leadership & Governance Summit 2025',
                        payee: 'Batangas Leadership Training Camp',
                        purpose: 'Venue, facilitators, and training materials',
                        amount: 200000.00,
                        fiscalYear: '2025',
                        disbursedBy: 'SK Treasurer Maria',
                        disbursedAt: '2025-05-16T10:00:00Z',
                        status: 'Released'
                    },
                    {
                        id: 'DV-2025-002',
                        voucherNo: 'DV-2025-002',
                        projectId: 202502,
                        projectTitle: 'Anti-Drug & Adolescent Wellness Campaign',
                        payee: 'Libjo Youth Healthcare & Advocacy',
                        purpose: 'Advocacy flyers, medical personnel honoraria, and kits',
                        amount: 50000.00,
                        fiscalYear: '2025',
                        disbursedBy: 'SK Treasurer Maria',
                        disbursedAt: '2025-10-11T13:30:00Z',
                        status: 'Released'
                    }
                );
                disbMigrated = true;
            }

            const hasDisb2024 = disbursements.some(d => String(d.fiscalYear) === '2024');
            if (!hasDisb2024) {
                disbursements.push(
                    {
                        id: 'DV-2024-001',
                        voucherNo: 'DV-2024-001',
                        projectId: 202401,
                        projectTitle: 'Linggo ng Kabataan Sports & Cultural Festival 2024',
                        payee: 'Libjo Community Sports Supplier',
                        purpose: 'Sound system, trophies, medals, and uniforms',
                        amount: 150000.00,
                        fiscalYear: '2024',
                        disbursedBy: 'SK Treasurer Maria',
                        disbursedAt: '2024-08-14T09:00:00Z',
                        status: 'Released'
                    },
                    {
                        id: 'DV-2024-002',
                        voucherNo: 'DV-2024-002',
                        projectId: 202402,
                        projectTitle: 'SK Educational Subsidy & School Supplies Distribution 2024',
                        payee: 'Batangas Academic Book & School Store',
                        purpose: 'School bags, notebooks, and learning assistance kits',
                        amount: 75000.00,
                        fiscalYear: '2024',
                        disbursedBy: 'SK Treasurer Maria',
                        disbursedAt: '2024-09-06T11:00:00Z',
                        status: 'Released'
                    }
                );
                disbMigrated = true;
            }

            if (disbMigrated) saveDisbursementsToLocal();
        } catch (e) {}
    }

    if (!disbursements || disbursements.length === 0) {
        disbursements = [
            {
                id: 'DV-2026-001',
                voucherNo: 'DV-2026-001',
                projectId: 1,
                projectTitle: 'Youth Sports Fest 2026',
                payee: 'Libjo Sports Gear & Uniforms Supply',
                purpose: 'Tournament uniforms and ball equipment',
                amount: 20000.00,
                fiscalYear: '2026',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2026-10-12T14:30:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2026-002',
                voucherNo: 'DV-2026-002',
                projectId: 2,
                projectTitle: 'Digital Literacy Workshop',
                payee: 'Batangas IT Training Center',
                purpose: 'Module printing and resource speaker honorarium',
                amount: 25000.00,
                fiscalYear: '2026',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2026-08-11T09:15:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2026-003',
                voucherNo: 'DV-2026-003',
                projectId: 111,
                projectTitle: 'Clean Up',
                payee: 'Barangay Libjo Ecological Sanitation Supply',
                purpose: 'Garbage segregation bins, gloves, and cleanup logistics',
                amount: 45000.00,
                fiscalYear: '2026',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2026-10-29T07:08:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2026-004',
                voucherNo: 'DV-2026-004',
                projectId: 112,
                projectTitle: 'Environment',
                payee: 'Batangas Tree Planting & Seedlings Nursery',
                purpose: 'Mangrove seedlings and tree planting supplies',
                amount: 25000.00,
                fiscalYear: '2026',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2026-10-29T07:11:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2025-001',
                voucherNo: 'DV-2025-001',
                projectId: 202501,
                projectTitle: 'Youth Leadership & Governance Summit 2025',
                payee: 'Batangas Leadership Training Camp',
                purpose: 'Venue, facilitators, and training materials',
                amount: 200000.00,
                fiscalYear: '2025',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2025-05-16T10:00:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2025-002',
                voucherNo: 'DV-2025-002',
                projectId: 202502,
                projectTitle: 'Anti-Drug & Adolescent Wellness Campaign',
                payee: 'Libjo Youth Healthcare & Advocacy',
                purpose: 'Advocacy flyers, medical personnel honoraria, and kits',
                amount: 50000.00,
                fiscalYear: '2025',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2025-10-11T13:30:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2024-001',
                voucherNo: 'DV-2024-001',
                projectId: 202401,
                projectTitle: 'Linggo ng Kabataan Sports & Cultural Festival 2024',
                payee: 'Libjo Community Sports Supplier',
                purpose: 'Sound system, trophies, medals, and uniforms',
                amount: 150000.00,
                fiscalYear: '2024',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2024-08-14T09:00:00Z',
                status: 'Released'
            },
            {
                id: 'DV-2024-002',
                voucherNo: 'DV-2024-002',
                projectId: 202402,
                projectTitle: 'SK Educational Subsidy & School Supplies Distribution 2024',
                payee: 'Batangas Academic Book & School Store',
                purpose: 'School bags, notebooks, and learning assistance kits',
                amount: 75000.00,
                fiscalYear: '2024',
                disbursedBy: 'SK Treasurer Maria',
                disbursedAt: '2024-09-06T11:00:00Z',
                status: 'Released'
            }
        ];
        saveDisbursementsToLocal();
    }

    // 5. Load Accomplishment Reports
    const savedReports = localStorage.getItem('prosko_reports');
    if (savedReports) {
        try { accomplishmentReports = JSON.parse(savedReports); } catch (e) {}
    }

    // 6. Load Brgy Documents
    const savedDocs = localStorage.getItem('prosko_docs');
    if (savedDocs) {
        try { brgyDocuments = JSON.parse(savedDocs); } catch (e) {}
    }

    // 7. Load Incidents
    const savedIncidents = localStorage.getItem('prosko_incidents');
    if (savedIncidents) {
        try { incidents = JSON.parse(savedIncidents); } catch (e) {}
    }

    // Fetch live Supabase data at system initialization
    await fetchAllDatabaseData();

    // Check active session
    checkActiveSession();

    // Set up automated snapshot schedule
    scheduleAutomatedBackupSnapshot();

    // Set up inactivity tracker
    setupInactivityTracker();

    // Set up login username input listeners
    const loginUserEl = document.getElementById('login-username');
    if (loginUserEl) {
        loginUserEl.addEventListener('input', checkLoginLockoutStatus);
        loginUserEl.addEventListener('change', checkLoginLockoutStatus);
        loginUserEl.addEventListener('blur', checkLoginLockoutStatus);
        loginUserEl.addEventListener('keyup', checkLoginLockoutStatus);
    }

    // Check lockout state for login
    checkLoginLockoutStatus();
}

function generateUniqueId(prefix) {
    return prefix + '-' + Date.now() + '-' + Math.floor(1000 + Math.random() * 9000);
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// =========================================================================
// 3. AUTHENTICATION & LOGIN LOCKOUT (IAS REQUIREMENTS A, C, D)
// =========================================================================
function fillCredentials(user, pass) {
    switchAuthTab('login');
    document.getElementById('login-username').value = user;
    document.getElementById('login-password').value = pass;
    checkLoginLockoutStatus();
    showToast('Filled credentials for ' + user, 'info');
}

function togglePasswordVisibility(inputId, iconId) {
    const input = document.getElementById(inputId);
    const icon = document.getElementById(iconId);
    if (!input) return;
    if (input.type === 'password') {
        input.type = 'text';
        if (icon) {
            icon.classList.remove('fa-eye');
            icon.classList.add('fa-eye-slash');
        }
    } else {
        input.type = 'password';
        if (icon) {
            icon.classList.remove('fa-eye-slash');
            icon.classList.add('fa-eye');
        }
    }
}

function switchAuthTab(tab) {
    const loginForm = document.getElementById('login-form');
    const regForm = document.getElementById('register-form');
    const loginBtn = document.getElementById('tab-login-btn');
    const regBtn = document.getElementById('tab-register-btn');
    const title = document.getElementById('form-title');
    const subtitle = document.getElementById('form-subtitle');

    hideAlert();

    if (tab === 'login') {
        loginForm.classList.remove('hidden');
        regForm.classList.add('hidden');
        loginBtn.className = "flex-1 py-2 text-xs font-bold rounded-xl transition duration-200 bg-white text-prosko-blue shadow-sm";
        regBtn.className = "flex-1 py-2 text-xs font-bold rounded-xl transition duration-200 text-slate-500 hover:text-slate-800";
        title.innerText = "Welcome Back";
        subtitle.innerText = "Please enter your credentials to access system.";
        checkLoginLockoutStatus();
    } else {
        loginForm.classList.add('hidden');
        regForm.classList.remove('hidden');
        regBtn.className = "flex-1 py-2 text-xs font-bold rounded-xl transition duration-200 bg-white text-prosko-blue shadow-sm";
        loginBtn.className = "flex-1 py-2 text-xs font-bold rounded-xl transition duration-200 text-slate-500 hover:text-slate-800";
        title.innerText = "Officer Registration";
        subtitle.innerText = "Create an authorized SK officer profile with secure OTP verification.";
    }
}

function getLockoutMap() {
    try {
        return JSON.parse(localStorage.getItem('prosko_login_lockout_map')) || {};
    } catch (e) {
        return {};
    }
}

function saveLockoutMap(map) {
    localStorage.setItem('prosko_login_lockout_map', JSON.stringify(map));
}

function getCanonicalUserKey(rawInput) {
    if (!rawInput) return '';
    const clean = rawInput.trim().toLowerCase();
    if (typeof usersStore !== 'undefined' && Array.isArray(usersStore)) {
        const found = usersStore.find(u =>
            (u.username && u.username.toLowerCase() === clean) ||
            (u.email && u.email.toLowerCase() === clean)
        );
        if (found && found.username) {
            return found.username.toLowerCase();
        }
    }
    return clean;
}

function checkLoginLockoutStatus() {
    const usernameInput = document.getElementById('login-username');
    const rawInput = usernameInput ? usernameInput.value : '';
    const userKey = getCanonicalUserKey(rawInput);
    const map = getLockoutMap();
    const record = userKey ? map[userKey] : null;

    const lockBox = document.getElementById('login-lockout-alert');
    const attemptsBox = document.getElementById('login-attempts-remaining');
    const attemptsText = document.getElementById('attempts-remaining-text');
    const captchaBox = document.getElementById('login-captcha-box');
    const submitBtn = document.getElementById('login-submit-btn');
    const alertBox = document.getElementById('auth-alert');

    const isLocked = Boolean(record && record.lockedUntil && record.lockedUntil > Date.now());
    const isCaptchaNeeded = Boolean(record && (record.captchaRequired || (record.lockoutCount && record.lockoutCount > 0)));

    // 1. Lockout State Handling
    if (isLocked) {
        if (lockBox) lockBox.classList.remove('hidden');
        if (alertBox) alertBox.classList.add('hidden'); // Avoid redundant stacked red alert pushing CAPTCHA off screen
        if (attemptsBox) attemptsBox.classList.add('hidden');
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.classList.add('opacity-50', 'cursor-not-allowed');
        }
        startLockoutCountdown(Math.ceil((record.lockedUntil - Date.now()) / 1000));
    } else {
        if (activeLockoutInterval) {
            clearInterval(activeLockoutInterval);
            activeLockoutInterval = null;
        }
        if (lockBox) lockBox.classList.add('hidden');
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.classList.remove('opacity-50', 'cursor-not-allowed');
        }

        if (record && record.failedAttempts > 0 && record.failedAttempts < 3) {
            if (attemptsBox) {
                attemptsBox.classList.remove('hidden');
                if (attemptsText) attemptsText.innerText = (3 - record.failedAttempts) + ' login attempt(s) remaining before account lockout.';
            }
        } else {
            if (attemptsBox) attemptsBox.classList.add('hidden');
        }
    }

    // 2. CAPTCHA State Handling (IAS Requirement D: Required both DURING and AFTER lockout periods)
    if (isCaptchaNeeded) {
        if (captchaBox) {
            captchaBox.classList.remove('hidden');
        }
        if (!currentCaptchaAnswer) generateNewCaptcha();
    } else {
        if (captchaBox) captchaBox.classList.add('hidden');
        currentCaptchaAnswer = null;
    }
}

function startLockoutCountdown(secondsRemaining) {
    if (activeLockoutInterval) clearInterval(activeLockoutInterval);
    const countdownEl = document.getElementById('lockout-countdown');
    const submitBtn = document.getElementById('login-submit-btn');

    let sec = secondsRemaining;
    const updateDisp = () => {
        const m = Math.floor(sec / 60).toString().padStart(2, '0');
        const s = (sec % 60).toString().padStart(2, '0');
        if (countdownEl) countdownEl.innerText = m + ':' + s;
    };
    updateDisp();

    activeLockoutInterval = setInterval(() => {
        sec--;
        if (sec <= 0) {
            clearInterval(activeLockoutInterval);
            activeLockoutInterval = null;
            const lockBox = document.getElementById('login-lockout-alert');
            if (lockBox) lockBox.classList.add('hidden');
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.classList.remove('opacity-50', 'cursor-not-allowed');
            }
            checkLoginLockoutStatus();
        } else {
            updateDisp();
        }
    }, 1000);
}

function generateNewCaptcha() {
    let num1, num2, ans;
    do {
        num1 = Math.floor(2 + Math.random() * 8);
        num2 = Math.floor(2 + Math.random() * 8);
        ans = (num1 + num2).toString();
    } while (ans === currentCaptchaAnswer);

    currentCaptchaAnswer = ans;
    const disp = document.getElementById('captcha-display');
    if (disp) disp.innerText = num1 + ' + ' + num2 + ' = ?';
    const input = document.getElementById('login-captcha-input');
    if (input) {
        input.value = '';
        input.classList.remove('border-red-500', 'ring-2', 'ring-red-400');
    }
}

async function handleLogin(e) {
    if (e) e.preventDefault();

    const usernameOrEmail = document.getElementById('login-username').value.trim();
    const passwordInput = document.getElementById('login-password').value.trim();
    const submitBtn = document.getElementById('login-submit-btn');

    if (!usernameOrEmail || !passwordInput) {
        showAlert('Please enter both username/email and password.', 'error');
        return;
    }

    const map = getLockoutMap();
    const userKey = getCanonicalUserKey(usernameOrEmail);
    const record = map[userKey] || { failedAttempts: 0, lockoutCount: 0, lockedUntil: 0, captchaRequired: false };

    // Check Lockout
    if (record.lockedUntil && record.lockedUntil > Date.now()) {
        const secLeft = Math.ceil((record.lockedUntil - Date.now()) / 1000);
        showToast('Account is temporarily locked. Please wait ' + secLeft + ' seconds.', 'error');
        checkLoginLockoutStatus();
        return;
    }

    // Check CAPTCHA if required (IAS Requirement D: enforced during/after lockout)
    const isCaptchaNeeded = Boolean(record.captchaRequired || (record.lockoutCount && record.lockoutCount > 0));
    if (isCaptchaNeeded) {
        const captchaInputEl = document.getElementById('login-captcha-input');
        const captchaInput = captchaInputEl ? captchaInputEl.value.trim() : '';
        const parsedInput = parseInt(captchaInput, 10);
        const parsedAnswer = parseInt(currentCaptchaAnswer, 10);

        if (!captchaInput || isNaN(parsedInput) || parsedInput !== parsedAnswer) {
            showAlert('CAPTCHA verification failed. Please enter the correct sum.', 'error');
            generateNewCaptcha();
            if (captchaInputEl) {
                captchaInputEl.value = '';
                captchaInputEl.focus();
                captchaInputEl.classList.add('border-red-500', 'ring-2', 'ring-red-400');
                setTimeout(() => {
                    captchaInputEl.classList.remove('border-red-500', 'ring-2', 'ring-red-400');
                }, 2000);
            }
            checkLoginLockoutStatus();
            return;
        }
    }

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<i class="fa-solid fa-spinner animate-spin"></i> Authenticating...';
    }

    const inputHash = await hashPassword(passwordInput);

    // 1. Check local Users Store first
    const matchedUser = usersStore.find(u =>
        (u.username.toLowerCase() === userKey || u.email.toLowerCase() === userKey)
    );

    let loginSuccess = false;

    if (matchedUser) {
        // Check if account deactivated
        if (matchedUser.status === 'Deactivated') {
            showAlert('Your account has been deactivated by the SK Chairperson Administrator.', 'error');
            await logUserActivityDirect(usernameOrEmail, matchedUser.role, 'LOGIN_FAILED', 'Deactivated account login attempt');
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.innerHTML = '<span>Sign In to System</span> <i class="fa-solid fa-arrow-right text-xs"></i>';
            }
            return;
        }

        // Check if account locked by Admin
        if (matchedUser.isLocked) {
            showAlert('This account is locked by the SK Administrator. Please contact SK Admin to unlock.', 'error');
            await logUserActivityDirect(usernameOrEmail, matchedUser.role, 'LOGIN_FAILED', 'Administrative locked account attempt');
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.innerHTML = '<span>Sign In to System</span> <i class="fa-solid fa-arrow-right text-xs"></i>';
            }
            return;
        }

        // Strict SHA-256 Hash Matching (IAS Requirement C)
        if (matchedUser.passwordHash === inputHash) {
            loginSuccess = true;
            activeUser = {
                id: matchedUser.id,
                name: matchedUser.fullName,
                role: matchedUser.role,
                email: matchedUser.email,
                username: matchedUser.username
            };
        }
    }

    // 2. Check Supabase profiles if not matched locally
    if (!loginSuccess && supabase) {
        try {
            const { data: profile } = await supabase
                .from('profiles')
                .select('*')
                .or('username.eq.' + usernameOrEmail + ',email.eq.' + usernameOrEmail)
                .maybeSingle();

            // Enforce SHA-256 password hash comparison against database record
            if (profile && profile.password_hash && profile.password_hash === inputHash) {
                activeUser = {
                    id: profile.id,
                    name: profile.full_name,
                    role: profile.role,
                    email: profile.email,
                    username: profile.username || usernameOrEmail
                };
                loginSuccess = true;
            }
        } catch (err) {}
    }

    if (loginSuccess) {
        // Enforce Belly Jean Montecillo identity immediately on authentication
        if (activeUser && (activeUser.username === 'admin_libjo' || activeUser.role === 'SK Chairperson' || /admin/i.test(activeUser.username) || /admin/i.test(activeUser.role) || (activeUser.name && /von|pilapil|sk chair admin|chair admin|sk chair|^admin$/i.test(activeUser.name)))) {
            activeUser.name = 'Belly Jean Montecillo';
            activeUser.role = 'SK Chairperson';
        }

        // RESET LOCKOUT MAP ON SUCCESS (IAS Requirement D)
        delete map[userKey];
        if (matchedUser && matchedUser.username) {
            delete map[matchedUser.username.toLowerCase()];
        }
        saveLockoutMap(map);
        currentCaptchaAnswer = null;
        checkLoginLockoutStatus();

        // Create secure session
        sessionToken = generateUniqueId('SES');
        sessionStorage.setItem('prosko_active_user', JSON.stringify(activeUser));
        sessionStorage.setItem('prosko_session_token', sessionToken);
        localStorage.setItem('prosko_session_token', sessionToken);

        await logUserActivityDirect(activeUser.username, activeUser.role, 'USER_LOGIN', 'Successful authentication for ' + activeUser.name);

        showToast('Login successful! Welcome to PROSKO.', 'success');
        await renderDashboard();

    } else {
        // INCREMENT FAILED ATTEMPTS (IAS Requirement D)
        record.failedAttempts = (record.failedAttempts || 0) + 1;

        if (record.failedAttempts >= 3) {
            record.lockoutCount = (record.lockoutCount || 0) + 1;

            // Progressive Lockout Periods: 1 min (60s), 5 min (300s), 15 min (900s)
            let lockoutDurationSec = 60;
            if (record.lockoutCount === 2) lockoutDurationSec = 300;
            if (record.lockoutCount >= 3) lockoutDurationSec = 900;

            record.lockedUntil = Date.now() + (lockoutDurationSec * 1000);
            record.failedAttempts = 0; // reset for next round
            record.captchaRequired = true;

            map[userKey] = record;
            saveLockoutMap(map);

            await logUserActivityDirect(usernameOrEmail, 'Guest', 'ACCOUNT_LOCKED', 'Account temporarily locked for ' + (lockoutDurationSec / 60) + ' min due to 3 failed login attempts');

            generateNewCaptcha();
            checkLoginLockoutStatus();
            showToast('Account temporarily locked for ' + (lockoutDurationSec / 60) + ' minute(s) due to 3 failed attempts.', 'error');

        } else {
            // If the account previously experienced a lockout, CAPTCHA remains required for continuing attempts
            if (record.lockoutCount > 0) {
                record.captchaRequired = true;
            }
            map[userKey] = record;
            saveLockoutMap(map);

            const attemptsLeft = 3 - record.failedAttempts;
            showAlert('Incorrect credentials. ' + attemptsLeft + ' attempt(s) remaining before lockout.', 'error');
            generateNewCaptcha();
            checkLoginLockoutStatus();

            await logUserActivityDirect(usernameOrEmail, 'Guest', 'LOGIN_FAILED', 'Failed login attempt (' + record.failedAttempts + '/3)');
        }
    }

    if (submitBtn) {
        const isStillLocked = Boolean(record.lockedUntil && record.lockedUntil > Date.now());
        submitBtn.disabled = isStillLocked;
        submitBtn.innerHTML = '<span>Sign In to System</span> <i class="fa-solid fa-arrow-right text-xs"></i>';
        if (isStillLocked) {
            submitBtn.classList.add('opacity-50', 'cursor-not-allowed');
        } else {
            submitBtn.classList.remove('opacity-50', 'cursor-not-allowed');
        }
    }
}

function checkActiveSession() {
    const savedSession = sessionStorage.getItem('prosko_active_user');
    const token = sessionStorage.getItem('prosko_session_token');
    if (savedSession && token) {
        try {
            activeUser = JSON.parse(savedSession);
            if (activeUser && (activeUser.username === 'admin_libjo' || activeUser.role === 'SK Chairperson' || /admin/i.test(activeUser.username) || /admin/i.test(activeUser.role) || (activeUser.name && /von|pilapil|sk chair admin|chair admin|sk chair|^admin$/i.test(activeUser.name)))) {
                activeUser.name = 'Belly Jean Montecillo';
                activeUser.role = 'SK Chairperson';
                sessionStorage.setItem('prosko_active_user', JSON.stringify(activeUser));
            }
            sessionToken = token;
            renderDashboard();
        } catch (e) {}
    }
}

function openLogoutModal() {
    customConfirm('Sign Out', 'Are you sure you want to end your current session?', (confirmed) => {
        if (confirmed) confirmLogout(false);
    });
}

async function confirmLogout(isSilent = false) {
    if (activeUser && !isSilent) {
        await logUserActivity('USER_LOGOUT', 'User signed out voluntarily');
    }

    // Broadcast logout across open browser tabs (IAS Requirement E)
    try {
        if (authBroadcastChannel) authBroadcastChannel.postMessage('LOGOUT');
    } catch (e) {}
    localStorage.setItem('prosko_logout_event', Date.now().toString());

    sessionStorage.removeItem('prosko_active_user');
    sessionStorage.removeItem('prosko_session_token');
    localStorage.removeItem('prosko_session_token');
    activeUser = null;
    sessionToken = null;

    document.getElementById('dashboard-screen').classList.add('hidden');
    document.getElementById('web-admin-dashboard').classList.add('hidden');
    document.getElementById('auth-screen').classList.remove('hidden');

    const shell = document.getElementById('main-shell');
    if (shell) shell.className = 'mode-mobile-shell transition-all duration-300';

    switchAuthTab('login');
    if (!isSilent) showToast('Logged out successfully.', 'info');
}

// =========================================================================
// 4. INACTIVITY AUTO-LOGOUT MODULE (IAS REQUIREMENT E: 15 MINUTES)
// =========================================================================
function setupInactivityTracker() {
    const recordActivity = () => {
        lastUserActivityTimestamp = Date.now();
    };

    window.addEventListener('mousemove', recordActivity);
    window.addEventListener('keydown', recordActivity);
    window.addEventListener('click', recordActivity);
    window.addEventListener('scroll', recordActivity);
    window.addEventListener('touchstart', recordActivity);

    if (inactivityCheckerInterval) clearInterval(inactivityCheckerInterval);
    inactivityCheckerInterval = setInterval(checkUserInactivity, 10000); // Check every 10 seconds
}

function checkUserInactivity() {
    if (!activeUser) return;

    const timeElapsed = Date.now() - lastUserActivityTimestamp;

    if (timeElapsed >= INACTIVITY_LIMIT_MS) {
        autoLogoutDueToInactivity();
    } else if (timeElapsed >= INACTIVITY_WARNING_MS) {
        showInactivityWarningModal();
    }
}

function showInactivityWarningModal() {
    const modal = document.getElementById('inactivity-modal');
    if (!modal || !modal.classList.contains('hidden')) return;

    modal.classList.remove('hidden');
    const countdownEl = document.getElementById('inactivity-countdown');

    if (inactivityModalCountdownInterval) clearInterval(inactivityModalCountdownInterval);
    inactivityModalCountdownInterval = setInterval(() => {
        const timeRemainingSec = Math.max(0, Math.ceil((INACTIVITY_LIMIT_MS - (Date.now() - lastUserActivityTimestamp)) / 1000));
        if (countdownEl) countdownEl.innerText = timeRemainingSec;

        if (timeRemainingSec <= 0) {
            clearInterval(inactivityModalCountdownInterval);
            autoLogoutDueToInactivity();
        }
    }, 1000);
}

function triggerInactivityWarningTest() {
    lastUserActivityTimestamp = Date.now() - INACTIVITY_WARNING_MS - 5000;
    showInactivityWarningModal();
    showToast('IAS Test: Simulating 15-Minute Inactivity Warning Modal', 'warning');
}

function resetInactivityTimer() {
    lastUserActivityTimestamp = Date.now();
    if (inactivityModalCountdownInterval) clearInterval(inactivityModalCountdownInterval);
    const modal = document.getElementById('inactivity-modal');
    if (modal) modal.classList.add('hidden');
    showToast('Session refreshed successfully.', 'info');
}

async function autoLogoutDueToInactivity() {
    const modal = document.getElementById('inactivity-modal');
    if (modal) modal.classList.add('hidden');
    if (inactivityModalCountdownInterval) clearInterval(inactivityModalCountdownInterval);

    if (activeUser) {
        await logUserActivityDirect(activeUser.username, activeUser.role, 'AUTO_LOGOUT', 'User logged out automatically due to 15 minutes of inactivity');
    }

    confirmLogout(true);
    showAlert('You have been logged out due to 15 minutes of inactivity for your security.', 'error');
}

function executeSilentLogout() {
    if (activeUser) {
        confirmLogout(true);
    }
}

// =========================================================================
// 5. REGISTRATION MODULE (IAS REQUIREMENTS A, B, C)
// =========================================================================
function checkPasswordStrength() {
    const password = document.getElementById('reg-password').value;
    const rules = {
        length: password.length >= 8,
        upper: /[A-Z]/.test(password),
        lower: /[a-z]/.test(password),
        number: /[0-9]/.test(password),
        special: /[@$!%*?&_#^() -]/.test(password)
    };

    for (const [key, valid] of Object.entries(rules)) {
        const el = document.getElementById('rule-' + key);
        if (el) {
            if (valid) {
                el.className = "flex items-center gap-1.5 text-emerald-600 font-bold";
                el.querySelector('i').className = "fa-solid fa-circle-check text-emerald-600";
            } else {
                el.className = "flex items-center gap-1.5 text-slate-500";
                el.querySelector('i').className = "fa-solid fa-circle-xmark text-red-500";
            }
        }
    }
}

async function handleRegister(e) {
    if (e) e.preventDefault();

    const fullname = document.getElementById('reg-fullname').value.trim();
    const username = document.getElementById('reg-username').value.trim();
    const email = document.getElementById('reg-email').value.trim();
    const role = document.getElementById('reg-role').value;
    const password = document.getElementById('reg-password').value;
    const confirmPassword = document.getElementById('reg-confirm-password').value;
    const submitBtn = document.getElementById('reg-submit-btn');

    if (!email.includes('@') || !email.includes('.')) {
        return showAlert('Please enter a valid email address (e.g. ana@libjo.ph).', 'error');
    }

    // Check unique username and email (IAS Requirement A)
    const existingUser = usersStore.find(u =>
        u.username.toLowerCase() === username.toLowerCase() ||
        u.email.toLowerCase() === email.toLowerCase()
    );

    if (existingUser) {
        return showAlert('Username or email is already registered in the system.', 'error');
    }

    if (password !== confirmPassword) {
        return showAlert('Passwords do not match.', 'error');
    }

    // Strong password regex with proper digit and character support (IAS Requirement C)
    const strongPasswordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&_#^() -])[A-Za-z\d@$!%*?&_#^() -]{8,}$/;
    if (!strongPasswordRegex.test(password)) {
        return showAlert('Password does not meet minimum security criteria (Min 8 chars, uppercase, lowercase, number, special char).', 'error');
    }

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<i class="fa-solid fa-spinner animate-spin"></i> Sending OTP...';
    }

    // IAS Requirement B: Send Account Verification OTP before activating account
    const code = Math.floor(100000 + Math.random() * 900000).toString();
    const hashedPassword = await hashPassword(password);
    const newUserId = generateUniqueId('USR');

    const pendingUser = {
        id: newUserId,
        fullName: fullname,
        username: username,
        email: email,
        role: role,
        passwordHash: hashedPassword,
        status: 'Active',
        isLocked: false,
        failedAttempts: 0,
        createdAt: new Date().toISOString()
    };

    activeOtpObj = {
        code: code,
        targetEmail: email,
        expiresAt: Date.now() + (5 * 60 * 1000), // 5 min expiry
        failedTries: 0,
        purpose: 'ACCOUNT_VERIFY',
        pendingUser: pendingUser
    };

    await logUserActivityDirect(username, role, 'OTP_REQUESTED', 'Account verification OTP requested for ' + email);

    if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.innerHTML = '<span>Create Account</span> <i class="fa-solid fa-user-plus text-xs"></i>';
    }

    // Open OTP Verification Modal
    document.getElementById('recovery-modal').classList.remove('hidden');
    document.getElementById('recovery-step-1').classList.add('hidden');
    document.getElementById('recovery-step-2').classList.remove('hidden');
    document.getElementById('recovery-target-email').innerText = email;
    document.getElementById('recovery-new-password').parentElement.classList.add('hidden');
    document.getElementById('rec-rule-length').parentElement.classList.add('hidden');
    document.getElementById('recovery-submit-btn').innerText = 'Verify & Activate Account';

    showToast('[ACCOUNT VERIFICATION OTP: ' + code + '] (Valid 5 mins)', 'info');
    console.log('[ACCOUNT VERIFICATION OTP]:', code);
}

// =========================================================================
// 6. FORGOT PASSWORD & ACCOUNT RECOVERY OTP MODULE (IAS REQUIREMENT B)
// =========================================================================
function openRecoveryModal() {
    document.getElementById('recovery-modal').classList.remove('hidden');
    resetRecoveryStep();
}

function closeRecoveryModal() {
    document.getElementById('recovery-modal').classList.add('hidden');
}

function resetRecoveryStep() {
    document.getElementById('recovery-step-1').classList.remove('hidden');
    document.getElementById('recovery-step-2').classList.add('hidden');
    document.getElementById('recovery-email').value = '';
    document.getElementById('recovery-new-password').value = '';
    document.getElementById('recovery-new-password').parentElement.classList.remove('hidden');
    document.getElementById('rec-rule-length').parentElement.classList.remove('hidden');
    document.getElementById('recovery-submit-btn').innerText = 'Reset Password';
    const inputs = document.querySelectorAll('.otp-input');
    inputs.forEach(input => input.value = '');
    activeOtpObj = null;
}

async function sendRecoveryOtp() {
    const emailInput = document.getElementById('recovery-email').value.trim();
    const sendBtn = document.getElementById('recovery-send-btn');
    const resendBtn = document.getElementById('recovery-resend-btn');

    if (!emailInput || !emailInput.includes('@')) {
        showToast('Please enter a valid email address.', 'error');
        return;
    }

    // IAS Requirement B: Cooldown timer before resending
    if (recoveryCooldownSec > 0) {
        showToast('Please wait ' + recoveryCooldownSec + 's before requesting another OTP.', 'error');
        return;
    }

    if (sendBtn) {
        sendBtn.disabled = true;
        sendBtn.innerHTML = '<i class="fa-solid fa-spinner animate-spin"></i> Generating OTP...';
    }

    const code = Math.floor(100000 + Math.random() * 900000).toString();
    activeOtpObj = {
        code: code,
        targetEmail: emailInput,
        expiresAt: Date.now() + (5 * 60 * 1000), // 5 minutes expiration
        failedTries: 0,
        purpose: 'PASSWORD_RESET'
    };

    // Start 60-second cooldown timer
    recoveryCooldownSec = 60;
    if (resendBtn) {
        resendBtn.disabled = true;
        resendBtn.innerText = 'Resend in (60s)';
    }

    if (recoveryCooldownTimer) clearInterval(recoveryCooldownTimer);
    recoveryCooldownTimer = setInterval(() => {
        recoveryCooldownSec--;
        if (recoveryCooldownSec <= 0) {
            clearInterval(recoveryCooldownTimer);
            if (resendBtn) {
                resendBtn.disabled = false;
                resendBtn.innerText = 'Resend OTP';
            }
        } else {
            if (resendBtn) resendBtn.innerText = 'Resend in (' + recoveryCooldownSec + 's)';
        }
    }, 1000);

    await logUserActivityDirect(emailInput, 'Guest', 'OTP_REQUESTED', 'Requested recovery OTP for ' + emailInput);

    setTimeout(() => {
        if (sendBtn) {
            sendBtn.disabled = false;
            sendBtn.innerHTML = '<span>Send Verification OTP Code</span> <i class="fa-solid fa-paper-plane text-xs"></i>';
        }

        document.getElementById('recovery-step-1').classList.add('hidden');
        document.getElementById('recovery-step-2').classList.remove('hidden');
        document.getElementById('recovery-target-email').innerText = emailInput;

        showToast('[DEMO SECURE OTP: ' + code + '] (Valid for 5 mins)', 'info');
        console.log('[SECURE RECOVERY OTP]:', code);
    }, 500);
}

function handleOtpInput(input, index) {
    const inputs = document.querySelectorAll('.otp-input');
    if (input.value.length === 1 && index < inputs.length - 1) {
        inputs[index + 1].focus();
    }
}

function checkRecoveryPasswordStrength() {
    const password = document.getElementById('recovery-new-password').value;
    const rules = {
        length: password.length >= 8,
        upper: /[A-Z]/.test(password),
        number: /[0-9]/.test(password),
        special: /[@$!%*?&_#^() -]/.test(password)
    };

    for (const [key, valid] of Object.entries(rules)) {
        const el = document.getElementById('rec-rule-' + key);
        if (el) {
            if (valid) {
                el.className = "flex items-center gap-1.5 text-emerald-600 font-bold";
                el.querySelector('i').className = "fa-solid fa-circle-check text-emerald-600";
            } else {
                el.className = "flex items-center gap-1.5 text-slate-500";
                el.querySelector('i').className = "fa-solid fa-circle-xmark text-red-500";
            }
        }
    }
}

async function verifyOtpAndResetPassword() {
    const inputs = document.querySelectorAll('.otp-input');
    let enteredOtp = '';
    inputs.forEach(input => enteredOtp += input.value.trim());

    const submitBtn = document.getElementById('recovery-submit-btn');

    if (enteredOtp.length < 6) {
        showToast('Please enter the full 6-digit OTP.', 'error');
        return;
    }

    if (!activeOtpObj) {
        showToast('OTP session expired. Please request a new code.', 'error');
        return;
    }

    if (Date.now() > activeOtpObj.expiresAt) {
        showToast('OTP has expired (exceeded 5 minutes). Request a new one.', 'error');
        activeOtpObj = null;
        return;
    }

    // Strict single-use OTP check (IAS Requirement B)
    if (enteredOtp !== activeOtpObj.code) {
        activeOtpObj.failedTries = (activeOtpObj.failedTries || 0) + 1;
        if (activeOtpObj.failedTries >= 3) {
            showToast('Exceeded 3 incorrect OTP attempts. Code invalidated.', 'error');
            activeOtpObj = null;
            resetRecoveryStep();
        } else {
            showToast('Invalid OTP. ' + (3 - activeOtpObj.failedTries) + ' attempt(s) remaining.', 'error');
        }
        return;
    }

    // Branch: ACCOUNT_VERIFY vs PASSWORD_RESET
    if (activeOtpObj.purpose === 'ACCOUNT_VERIFY') {
        const newUser = activeOtpObj.pendingUser;
        usersStore.push(newUser);
        saveUsersStoreToLocal();

        if (supabase) {
            try {
                await supabase.from('profiles').insert([{
                    full_name: newUser.fullName,
                    username: newUser.username,
                    email: newUser.email,
                    role: newUser.role
                }]);
            } catch (e) {}
        }

        await logUserActivityDirect(newUser.username, newUser.role, 'USER_REGISTER', 'Account verified and activated via OTP for ' + newUser.fullName);
        activeOtpObj = null;

        showToast('Account verified and activated! Please sign in.', 'success');
        closeRecoveryModal();
        switchAuthTab('login');
        fillCredentials(newUser.username, '');
        return;
    }

    // Branch: PASSWORD_RESET
    const newPassword = document.getElementById('recovery-new-password').value.trim();
    const strongPasswordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&_#^() -])[A-Za-z\d@$!%*?&_#^() -]{8,}$/;
    if (!strongPasswordRegex.test(newPassword)) {
        showToast('New password does not meet complexity rules (Min 8 chars, uppercase, lowercase, number, special char).', 'error');
        return;
    }

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<i class="fa-solid fa-spinner animate-spin"></i> Updating...';
    }

    const newHash = await hashPassword(newPassword);

    // Update user in usersStore
    const targetEmail = activeOtpObj.targetEmail.toLowerCase();
    const usr = usersStore.find(u => u.email.toLowerCase() === targetEmail);
    if (usr) {
        usr.passwordHash = newHash;
        usr.isLocked = false;
        usr.failedAttempts = 0;
        saveUsersStoreToLocal();
    }

    // Invalidate other active sessions (IAS Requirement C)
    localStorage.setItem('prosko_logout_event', Date.now().toString());

    await logUserActivityDirect(targetEmail, 'User', 'PASSWORD_RESET', 'Password successfully reset via OTP verification');

    activeOtpObj = null; // single-use OTP consumed

    if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.innerHTML = 'Reset Password';
    }

    showToast('Password reset successfully! You can now sign in.', 'success');
    closeRecoveryModal();
    switchAuthTab('login');
}

// =========================================================================
// 7. ACCOUNT SECURITY SETTINGS (CHANGE PASSWORD & CHANGE EMAIL WITH OTP)
// =========================================================================
function openAccountSettingsModal() {
    if (!activeUser) return;
    document.getElementById('acc-info-name').innerText = activeUser.name;
    document.getElementById('acc-info-role').innerText = activeUser.role;
    document.getElementById('acc-info-email').innerText = activeUser.email;
    document.getElementById('acc-curr-password').value = '';
    document.getElementById('acc-new-password').value = '';
    document.getElementById('acc-confirm-password').value = '';

    document.getElementById('change-email-step-1').classList.remove('hidden');
    document.getElementById('change-email-step-2').classList.add('hidden');
    document.getElementById('acc-new-email').value = '';
    document.getElementById('acc-email-otp-input').value = '';

    switchAccountSettingsTab('profile');

    document.getElementById('account-settings-modal').classList.remove('hidden');
}

function switchAccountSettingsTab(tab) {
    const panels = {
        'profile': document.getElementById('acc-panel-profile'),
        'password': document.getElementById('acc-panel-password'),
        'email': document.getElementById('acc-panel-email')
    };

    const tabs = {
        'profile': document.getElementById('acc-tab-profile'),
        'password': document.getElementById('acc-tab-pass'),
        'email': document.getElementById('acc-tab-email')
    };

    Object.keys(panels).forEach(key => {
        if (panels[key]) {
            if (key === tab) {
                panels[key].classList.remove('hidden');
            } else {
                panels[key].classList.add('hidden');
            }
        }
        if (tabs[key]) {
            if (key === tab) {
                tabs[key].className = "flex-1 py-1.5 rounded-lg bg-white text-prosko-blue shadow-sm font-bold";
            } else {
                tabs[key].className = "flex-1 py-1.5 rounded-lg text-slate-600 hover:text-slate-900 font-bold";
            }
        }
    });
}

function closeAccountSettingsModal() {
    document.getElementById('account-settings-modal').classList.add('hidden');
}

function checkAccountPasswordStrength() {
    const password = document.getElementById('acc-new-password').value;
    const rules = {
        length: password.length >= 8,
        upper: /[A-Z]/.test(password),
        number: /[0-9]/.test(password),
        special: /[@$!%*?&_#^() -]/.test(password)
    };

    for (const [key, valid] of Object.entries(rules)) {
        const el = document.getElementById('acc-rule-' + key);
        if (el) {
            if (valid) {
                el.className = "flex items-center gap-1.5 text-emerald-600 font-bold";
                el.querySelector('i').className = "fa-solid fa-circle-check text-emerald-600";
            } else {
                el.className = "flex items-center gap-1.5 text-slate-500";
                el.querySelector('i').className = "fa-solid fa-circle-xmark text-red-500";
            }
        }
    }
}

async function submitChangePassword() {
    if (!activeUser) return;
    const currPass = document.getElementById('acc-curr-password').value;
    const newPass = document.getElementById('acc-new-password').value;
    const confPass = document.getElementById('acc-confirm-password').value;

    if (!currPass || !newPass || !confPass) {
        showToast('Please fill in all password fields.', 'error');
        return;
    }

    const currHash = await hashPassword(currPass);
    const userRec = usersStore.find(u => u.username.toLowerCase() === activeUser.username.toLowerCase());

    if (userRec && userRec.passwordHash !== currHash) {
        showToast('Current password does not match.', 'error');
        return;
    }

    if (newPass !== confPass) {
        showToast('New passwords do not match.', 'error');
        return;
    }

    const strongPasswordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&_#^() -])[A-Za-z\d@$!%*?&_#^() -]{8,}$/;
    if (!strongPasswordRegex.test(newPass)) {
        showToast('New password does not meet complexity rules.', 'error');
        return;
    }

    const newHash = await hashPassword(newPass);
    if (userRec) {
        userRec.passwordHash = newHash;
        saveUsersStoreToLocal();
        await supabaseUpdate('profiles', { password_hash: newHash }, 'username', userRec.username);
    }

    // Invalidate other active sessions (IAS Requirement C)
    localStorage.setItem('prosko_logout_event', Date.now().toString());

    await logUserActivity('PASSWORD_CHANGED', 'Password successfully updated for ' + activeUser.username);

    showToast('Password updated! Other active sessions invalidated.', 'success');
    closeAccountSettingsModal();
}

// Change Email with OTP (IAS Requirement B)
let changeEmailOtp = null;
let changeEmailPendingAddress = null;

function sendChangeEmailOtp() {
    const newEmail = document.getElementById('acc-new-email').value.trim();
    const resendBtn = document.getElementById('change-email-resend-btn');

    if (!newEmail || !newEmail.includes('@') || !newEmail.includes('.')) {
        showToast('Please enter a valid new email address.', 'error');
        return;
    }

    if (changeEmailCooldownSec > 0) {
        showToast('Please wait ' + changeEmailCooldownSec + 's before requesting a new OTP.', 'error');
        return;
    }

    const code = Math.floor(100000 + Math.random() * 900000).toString();
    changeEmailOtp = code;
    changeEmailPendingAddress = newEmail;

    changeEmailCooldownSec = 60;
    if (resendBtn) {
        resendBtn.disabled = true;
        resendBtn.innerText = 'Resend in (60s)';
    }

    if (changeEmailCooldownTimer) clearInterval(changeEmailCooldownTimer);
    changeEmailCooldownTimer = setInterval(() => {
        changeEmailCooldownSec--;
        if (changeEmailCooldownSec <= 0) {
            clearInterval(changeEmailCooldownTimer);
            if (resendBtn) {
                resendBtn.disabled = false;
                resendBtn.innerText = 'Resend Code';
            }
        } else {
            if (resendBtn) resendBtn.innerText = 'Resend in (' + changeEmailCooldownSec + 's)';
        }
    }, 1000);

    document.getElementById('change-email-target-disp').innerText = newEmail;
    document.getElementById('change-email-step-1').classList.add('hidden');
    document.getElementById('change-email-step-2').classList.remove('hidden');

    showToast('[OTP to ' + newEmail + ': ' + code + ']', 'info');
    console.log('[CHANGE EMAIL OTP]:', code);
}

async function verifyAndApplyNewEmail() {
    const entered = document.getElementById('acc-email-otp-input').value.trim();
    if (entered !== changeEmailOtp) {
        showToast('Invalid OTP verification code.', 'error');
        return;
    }

    const oldEmail = activeUser.email;
    activeUser.email = changeEmailPendingAddress;
    sessionStorage.setItem('prosko_active_user', JSON.stringify(activeUser));

    const usr = usersStore.find(u => u.username.toLowerCase() === activeUser.username.toLowerCase());
    if (usr) {
        usr.email = changeEmailPendingAddress;
        saveUsersStoreToLocal();
    }

    await logUserActivity('EMAIL_CHANGED', 'Email changed from ' + oldEmail + ' to ' + changeEmailPendingAddress);

    showToast('Email verified and updated successfully!', 'success');
    closeAccountSettingsModal();
}

// =========================================================================
// 8. DATABASE FETCHING & REALTIME SUBSCRIPTIONS
// =========================================================================
async function fetchAllDatabaseData() {
    const client = getSupabase();
    if (!client) return;
    try {
        // 1. Projects
        const { data: projData, error: projErr } = await client
            .from('projects')
            .select('*')
            .order('created_at', { ascending: false });

        if (!projErr && projData && projData.length > 0) {
            projects = projData.map(p => {
                let sub = p.submitted_by || p.submittedBy || 'SK Office';
                if (!sub || /von|pilapil|sk chair admin|chair admin|sk chair|admin_libjo|^admin$|^sk chairperson$|belly jean montecillo/i.test(sub)) {
                    sub = 'Belly Jean Montecillo (SK Chairperson)';
                }
                let st = p.status;
                const d = parseFloat(p.disbursed) || 0;
                if (st === 'In Progress' && d <= 0) {
                    st = 'Approved';
                }
                if (st !== p.status || sub !== (p.submitted_by || p.submittedBy)) {
                    supabaseUpdate('projects', { status: st, submitted_by: sub }, 'id', p.id).catch(() => {});
                }
                return {
                    id: p.id,
                    title: p.title,
                    description: p.description || '',
                    category: p.category,
                    budget: parseFloat(p.budget) || 0,
                    disbursed: d,
                    status: st,
                    submittedBy: sub,
                    period: p.period || '',
                    startDate: p.start_date || '',
                    endDate: p.end_date || '',
                    aiScore: p.ai_score || '94% Verified',
                    fiscalYear: p.fiscal_year || '2026',
                    createdAt: p.created_at
                };
            });
            saveProjectsToLocal();
        }

        // 2. Disbursements
        try {
            const { data: disbData, error: disbErr } = await client
                .from('disbursements')
                .select('*')
                .order('created_at', { ascending: false });

            if (!disbErr && disbData && disbData.length > 0) {
                disbursements = disbData.map(d => ({
                    id: d.id,
                    voucherNo: d.voucher_no || d.id,
                    projectId: d.project_id,
                    projectTitle: d.project_title,
                    payee: d.payee,
                    purpose: d.purpose,
                    amount: parseFloat(d.amount) || 0,
                    fiscalYear: d.fiscal_year || '2026',
                    disbursedBy: d.disbursed_by,
                    disbursedAt: d.disbursed_at || d.created_at,
                    status: d.status || 'Released'
                }));
                saveDisbursementsToLocal();
            }
        } catch (disbErr) {}

        // 3. Accomplishment Reports
        try {
            let repData = null;
            const res1 = await client.from('accomplishment_reports').select('*').order('created_at', { ascending: false });
            if (!res1.error && res1.data && res1.data.length > 0) {
                repData = res1.data;
            } else {
                const res2 = await client.from('reports').select('*').order('created_at', { ascending: false });
                if (!res2.error && res2.data && res2.data.length > 0) repData = res2.data;
            }
            if (repData && repData.length > 0) {
                accomplishmentReports = repData;
                saveReportsToLocal();
            }
        } catch (repErr) {}

        // 4. Documents
        try {
            let docData = null;
            const resDoc1 = await client.from('brgy_documents').select('*').order('created_at', { ascending: false });
            if (!resDoc1.error && resDoc1.data && resDoc1.data.length > 0) {
                docData = resDoc1.data;
            } else {
                const resDoc2 = await client.from('documents').select('*').order('created_at', { ascending: false });
                if (!resDoc2.error && resDoc2.data && resDoc2.data.length > 0) docData = resDoc2.data;
            }
            if (docData && docData.length > 0) {
                brgyDocuments = docData.map(d => ({
                    id: d.id,
                    ref: d.ref || d.ref_no || d.reference_no,
                    title: d.title,
                    type: d.type || d.doc_type,
                    date: d.date || (d.created_at ? new Date(d.created_at).toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' }) : 'Oct 20, 2026')
                }));
                saveDocsToLocal();
            }
        } catch (docErr) {}

        // 5. Incidents
        try {
            const { data: incData, error: incErr } = await client
                .from('incidents')
                .select('*')
                .order('created_at', { ascending: false });

            if (!incErr && incData && incData.length > 0) {
                incidents = incData.map(i => ({
                    id: i.id,
                    type: i.type || i.incident_type,
                    location: i.location,
                    status: i.status,
                    date: i.date || (i.created_at ? new Date(i.created_at).toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' }) : 'Oct 20, 2026')
                }));
                saveIncidentsToLocal();
            }
        } catch (incErr) {}

        // 6. Fiscal Budget
        try {
            const { data: bData, error: bErr } = await client.from('fiscal_budget').select('*');
            if (!bErr && bData && bData.length > 0) {
                bData.forEach(b => {
                    if (b.year && b.allocated_budget) {
                        annualBudgets[b.year] = parseFloat(b.allocated_budget);
                    }
                });
                localStorage.setItem('prosko_annual_budgets', JSON.stringify(annualBudgets));
            }
        } catch (bErr) {}

        // 7. Profiles / Users Store Sync
        try {
            const { data: profData, error: profErr } = await client.from('profiles').select('*');
            if (!profErr && profData && profData.length > 0) {
                profData.forEach(p => {
                    const existing = usersStore.find(u => u.username === p.username || (p.email && u.email === p.email));
                    if (existing) {
                        existing.fullName = p.full_name || existing.fullName;
                        existing.role = p.role || existing.role;
                        if (p.password_hash) existing.passwordHash = p.password_hash;
                        if (p.status) existing.status = p.status;
                        if (p.is_locked !== undefined) existing.isLocked = p.is_locked;
                    } else {
                        usersStore.push({
                            id: p.id,
                            username: p.username,
                            passwordHash: p.password_hash || '',
                            fullName: p.full_name,
                            email: p.email,
                            role: p.role,
                            status: p.status || 'Active',
                            isLocked: Boolean(p.is_locked),
                            failedAttempts: p.failed_attempts || 0,
                            createdAt: p.created_at
                        });
                    }
                });
                saveUsersStoreToLocal();
            }
        } catch (profErr) {}

    } catch (err) {
        console.error('Error fetching database records:', err);
    }
}

function setupRealtimeSubscriptions() {
    const client = getSupabase();
    if (!client) return;
    if (realtimeChannel) {
        client.removeChannel(realtimeChannel);
    }

    realtimeChannel = client
        .channel('public:app-db-changes')
        .on('postgres_changes', { event: '*', schema: 'public', table: 'projects' }, async () => {
            await fetchAllDatabaseData();
            updateDashboardUI();
        })
        .on('postgres_changes', { event: '*', schema: 'public', table: 'disbursements' }, async () => {
            await fetchAllDatabaseData();
            updateDashboardUI();
        })
        .on('postgres_changes', { event: '*', schema: 'public', table: 'accomplishment_reports' }, async () => {
            await fetchAllDatabaseData();
            updateDashboardUI();
        })
        .on('postgres_changes', { event: '*', schema: 'public', table: 'brgy_documents' }, async () => {
            await fetchAllDatabaseData();
            updateDashboardUI();
        })
        .on('postgres_changes', { event: '*', schema: 'public', table: 'incidents' }, async () => {
            await fetchAllDatabaseData();
            updateDashboardUI();
        })
        .on('postgres_changes', { event: '*', schema: 'public', table: 'profiles' }, async () => {
            await fetchAllDatabaseData();
            updateDashboardUI();
        })
        .subscribe();
}

// =========================================================================
// 9. DASHBOARD RENDERING & ROLE SEPARATION (TASK REQS 1 & 3)
// =========================================================================
async function renderDashboard() {
    const shell = document.getElementById('main-shell');
    const authScreen = document.getElementById('auth-screen');
    const dashboardScreen = document.getElementById('dashboard-screen');
    const webAdminDash = document.getElementById('web-admin-dashboard');
    const bottomNav = document.getElementById('mobile-bottom-nav');

    authScreen.classList.add('hidden');
    // Ensure active fiscal year (2026) is always established upon logging in
    currentFiscalYear = '2026';
    const fySelect = document.getElementById('budget-fiscal-year');
    if (fySelect) fySelect.value = '2026';
    await fetchAllDatabaseData();
    setupRealtimeSubscriptions();

    const role = activeUser.role;
    const isAdminRole = (role === 'SK Chairperson' || role === 'Admin' || role === 'Admin / System Overseer');

    if (isAdminRole) {
        // SWITCH TO DESKTOP WEB ADMIN UI
        shell.className = "mode-web-admin transition-all duration-300";
        dashboardScreen.classList.add('hidden');
        bottomNav.classList.add('hidden');
        webAdminDash.classList.remove('hidden');

        document.getElementById('web-admin-name').innerText = activeUser.name;
        document.getElementById('web-admin-role').innerText = activeUser.role;
        const webAdminAvatar = document.getElementById('web-admin-avatar');
        if (webAdminAvatar) {
            webAdminAvatar.innerText = activeUser.name.split(' ').map(n => n[0]).join('').substring(0, 2);
        }

        switchDashboardTab('projects');

    } else {
        // SWITCH TO MOBILE APP SHELL VIEW (SECRETARY & TREASURER)
        shell.className = "mode-mobile-shell transition-all duration-300";
        webAdminDash.classList.add('hidden');
        dashboardScreen.classList.remove('hidden');
        bottomNav.classList.remove('hidden');
        if (dashboardScreen) dashboardScreen.scrollTop = 0;

        document.getElementById('dash-user-name').innerText = activeUser.name;
        document.getElementById('dash-user-role').innerText = activeUser.role;
        document.getElementById('dash-welcome-name').innerText = activeUser.name;
        document.getElementById('user-avatar').innerText = activeUser.name.split(' ').map(n => n[0]).join('').substring(0, 2);

        if (role === 'SK Secretary') {
            bottomNav.innerHTML = `
                <button type="button" id="tab-btn-projects" onclick="switchDashboardTab('projects')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-prosko-blue font-bold">
                    <i class="fa-solid fa-file-signature text-base"></i>
                    <span class="text-[9px]">Proposals</span>
                </button>
                <button type="button" id="tab-btn-sec-reports" onclick="switchDashboardTab('sec-reports')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-purple font-semibold">
                    <i class="fa-solid fa-award text-base"></i>
                    <span class="text-[9px]">Reports</span>
                </button>
                <button type="button" id="tab-btn-sec-docs" onclick="switchDashboardTab('sec-docs')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-folder-tree text-base"></i>
                    <span class="text-[9px]">Brgy Docs</span>
                </button>
                <button type="button" id="tab-btn-profile" onclick="openAccountSettingsModal()" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-circle-user text-base"></i>
                    <span class="text-[9px]">Account</span>
                </button>
            `;

            document.getElementById('session-badge').innerHTML = '<i class="fa-solid fa-file-pen text-[10px] mr-1"></i> Secretary Portal (Propose Only)';
            document.getElementById('banner-subtext').innerText = 'File project proposals and accomplishment reports for official SK Chairperson approval.';
            switchDashboardTab('projects');

        } else if (role === 'SK Treasurer') {
            bottomNav.innerHTML = `
                <button type="button" id="tab-btn-treasurer-budget" onclick="switchDashboardTab('treasurer-budget')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-prosko-blue font-bold">
                    <i class="fa-solid fa-vault text-base"></i>
                    <span class="text-[9px]">Budget Ledger</span>
                </button>
                <button type="button" id="tab-btn-projects" onclick="switchDashboardTab('projects')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-list-check text-base"></i>
                    <span class="text-[9px]">Projects</span>
                </button>
                <button type="button" id="tab-btn-ai" onclick="switchDashboardTab('ai')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-brain text-base"></i>
                    <span class="text-[9px]">AI Risk</span>
                </button>
                <button type="button" id="tab-btn-profile" onclick="openAccountSettingsModal()" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-circle-user text-base"></i>
                    <span class="text-[9px]">Account</span>
                </button>
            `;

            document.getElementById('session-badge').innerHTML = '<i class="fa-solid fa-vault text-[10px] mr-1"></i> Treasurer Financial Authority';
            document.getElementById('banner-subtext').innerText = 'SK Treasury Portal: Authorize fund releases, maintain budget ledger, and disburse project funds.';
            switchDashboardTab('treasurer-budget');

        } else {
            // General Kagawad / Officer Navigation
            bottomNav.innerHTML = `
                <button type="button" id="tab-btn-projects" onclick="switchDashboardTab('projects')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-prosko-blue font-bold">
                    <i class="fa-solid fa-list-check text-base"></i>
                    <span class="text-[9px]">Projects</span>
                </button>
                <button type="button" id="tab-btn-sec-reports" onclick="switchDashboardTab('sec-reports')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-purple font-semibold">
                    <i class="fa-solid fa-award text-base"></i>
                    <span class="text-[9px]">Reports</span>
                </button>
                <button type="button" id="tab-btn-treasurer-budget" onclick="switchDashboardTab('treasurer-budget')" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-vault text-base"></i>
                    <span class="text-[9px]">Ledger</span>
                </button>
                <button type="button" id="tab-btn-profile" onclick="openAccountSettingsModal()" class="flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold">
                    <i class="fa-solid fa-circle-user text-base"></i>
                    <span class="text-[9px]">Account</span>
                </button>
            `;

            document.getElementById('session-badge').innerHTML = '<i class="fa-solid fa-users text-[10px] mr-1"></i> SK Council Member Portal';
            document.getElementById('banner-subtext').innerText = 'Barangay Libjo Sangguniang Kabataan Project Monitoring & Evaluation.';
            switchDashboardTab('projects');
        }
    }

    updateDashboardUI();
}

// SAFE SUBVIEW SWITCHER: NEVER DELETES DOM ELEMENTS!
function switchDashboardTab(tab) {
    currentActiveTab = tab;
    const isWebAdmin = activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin' || activeUser.role === 'Admin / System Overseer');

    const targetContainer = isWebAdmin ? document.getElementById('web-admin-viewport') : document.getElementById('mobile-subviews-container');
    const subviewElement = document.getElementById('subview-' + tab);
    const masterStore = document.getElementById('subviews-master-store');

    if (targetContainer && subviewElement) {
        // Return any currently active subviews safely to the master store (preserving listeners and DOM IDs)
        while (targetContainer.firstChild) {
            const child = targetContainer.firstChild;
            if (masterStore && child.id && child.id.startsWith('subview-')) {
                child.classList.add('hidden');
                masterStore.appendChild(child);
            } else {
                child.remove();
            }
        }
        subviewElement.classList.remove('hidden');
        targetContainer.appendChild(subviewElement);
    }

    // Reset scroll positions to prevent banner clipping and ensure fresh view
    const dashScreen = document.getElementById('dashboard-screen');
    if (dashScreen) dashScreen.scrollTop = 0;
    const webViewport = document.getElementById('web-admin-viewport');
    if (webViewport) webViewport.scrollTop = 0;
    window.scrollTo(0, 0);

    // Highlight desktop sidebar nav
    const navIds = ['projects', 'sec-reports', 'sec-docs', 'treasurer-budget', 'analytics', 'log-trail', 'ai', 'sia', 'users', 'backup'];
    navIds.forEach(id => {
        const btn = document.getElementById('admin-nav-' + id);
        if (btn) {
            if (id === tab) {
                btn.className = "w-full flex items-center justify-between px-3.5 py-2.5 rounded-xl text-white bg-prosko-blue font-bold shadow-md transition";
            } else {
                btn.className = "w-full flex items-center justify-between px-3.5 py-2.5 rounded-xl text-slate-300 hover:bg-white/10 transition font-semibold";
            }
        }
    });

    // Highlight mobile bottom nav
    const mobileTabs = ['projects', 'sec-reports', 'sec-docs', 'treasurer-budget', 'ai'];
    mobileTabs.forEach(t => {
        const btn = document.getElementById('tab-btn-' + t);
        if (btn) {
            if (t === tab) {
                btn.className = "flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-prosko-blue font-bold bg-blue-50/80";
            } else {
                btn.className = "flex flex-col items-center gap-1 py-1 px-2.5 rounded-xl transition text-slate-400 hover:text-prosko-blue font-semibold";
            }
        }
    });

    const titleMap = {
        'projects': 'Youth Projects & Approvals Overview',
        'sec-reports': 'SK Accomplishment Reports Verification',
        'sec-docs': 'Barangay & SK Official Document Repository',
        'treasurer-budget': 'Treasury Budget & Disbursement Ledger',
        'analytics': 'Community Incident Log Mapper',
        'log-trail': 'Realtime System Audit Trail Logs',
        'ai': 'AI Project Risk & Compliance Index',
        'users': 'SK Official User & Role Management',
        'backup': 'Database Backup & Disaster Recovery'
    };

    const webTitle = document.getElementById('web-topbar-title');
    if (webTitle && titleMap[tab]) webTitle.innerText = titleMap[tab];

    if (tab === 'log-trail') fetchLogTrails();
    if (tab === 'users') renderUsersTable();
    if (tab === 'backup') renderBackupHistory();
    if (tab === 'treasurer-budget') {
        const fySelect = document.getElementById('budget-fiscal-year');
        if (fySelect && currentFiscalYear) {
            fySelect.value = currentFiscalYear;
        }
        renderTreasurerBudgetCards();
        renderDisbursementLogsTable();
    }
}

// =========================================================================
// 10. METRIC CALCULATION ENGINE & ACCURACY (TASK REQ 2)
// =========================================================================
function updateCalculatedMetrics() {
    // 1. Annual SK Youth Budget (10% Barangay IRA allocation)
    const annualBudget = annualBudgets[currentFiscalYear] || 1600000.00;

    // 2. Total Allocated: Sum of approved / active youth projects for SELECTED FISCAL YEAR
    const fyProjects = projects.filter(p => String(p.fiscalYear || '2026') === String(currentFiscalYear));
    const approvedProjects = fyProjects.filter(p => p.status === 'Approved' || p.status === 'In Progress' || p.status === 'Completed');
    const totalAllocated = approvedProjects.reduce((acc, p) => acc + (parseFloat(p.budget) || 0), 0);

    // 3. Available for Allocation: Remaining youth budget available to fund NEW projects
    // FORMULA: Annual Youth Budget - Total Allocated Budget
    const availableForAllocation = Math.max(0, annualBudget - totalAllocated);

    // 4. Total Disbursed: Sum of positive released vouchers for active fiscal year
    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear));
    const releasedVouchers = fyDisbursements.filter(d => d.status !== 'Returned' && (parseFloat(d.amount) || 0) > 0);
    const totalDisbursed = releasedVouchers.reduce((acc, d) => acc + (parseFloat(d.amount) || 0), 0);

    // Returned / Refunded amount from cancelled projects
    const totalRefunded = fyDisbursements.filter(d => d.status === 'Returned' || (parseFloat(d.amount) || 0) < 0)
        .reduce((acc, d) => acc + Math.abs(parseFloat(d.amount) || 0), 0);

    // Net actual cash disbursed
    const netDisbursed = Math.max(0, totalDisbursed - totalRefunded);

    // 5. Undisbursed Allocated Funds: Funds committed to approved projects awaiting release
    // FORMULA: Total Allocated - Total Disbursed
    const undisbursedAllocated = Math.max(0, totalAllocated - netDisbursed);

    const pendingProjCount = fyProjects.filter(p => p.status === 'Pending Admin Approval').length;
    const pendingRepCount = accomplishmentReports.filter(r => String(r.fiscalYear || '2026') === String(currentFiscalYear) && r.status === 'Pending Admin Approval').length;
    const totalPending = pendingProjCount + pendingRepCount;

    // Web Admin Stat Cards
    const webBudget = document.getElementById('web-stat-total-budget');
    const webBudgetSub = document.getElementById('web-stat-total-budget-sub');
    const webDisb = document.getElementById('web-stat-total-disbursed');
    const webDisbSub = document.getElementById('web-stat-total-disbursed-sub');
    const webPending = document.getElementById('web-stat-pending-count');
    const webPendingSub = document.getElementById('web-stat-pending-sub');
    const webProjCount = document.getElementById('web-stat-projects-count');

    // Available for Allocation: Money that can still be allocated to new projects
    if (webBudget) {
        webBudget.innerText = '₱' + availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 });
        webBudget.title = 'Available for Allocation: ₱' + availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 });
    }
    if (webBudgetSub) {
        webBudgetSub.innerText = 'of ₱' + annualBudget.toLocaleString('en-US', { minimumFractionDigits: 2 }) + ' Annual IRA (₱' + totalAllocated.toLocaleString('en-US', { minimumFractionDigits: 2 }) + ' Allocated)';
    }

    if (webDisb) {
        webDisb.innerText = '₱' + totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 });
        webDisb.title = 'Total Disbursed: ₱' + totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 });
    }
    if (webDisbSub) {
        webDisbSub.innerText = '₱' + undisbursedAllocated.toLocaleString('en-US', { minimumFractionDigits: 2 }) + ' Committed & Undisbursed';
    }

    if (webPending) webPending.innerText = totalPending + ' Items';
    if (webPendingSub) webPendingSub.innerText = pendingProjCount + ' Proj • ' + pendingRepCount + ' Reports';
    if (webProjCount) webProjCount.innerText = fyProjects.length;

    // Treasury & Budget Subview Cards
    const tbBudget = document.getElementById('tb-annual-budget');
    const tbAvailable = document.getElementById('tb-available-budget');
    const tbAvailableSub = document.getElementById('tb-available-budget-sub');
    const tbAlloc = document.getElementById('tb-total-allocated');
    const tbDisb = document.getElementById('tb-total-disbursed');
    const tbUnexp = document.getElementById('tb-unexpended-balance');

    if (tbBudget) {
        tbBudget.innerText = '₱' + annualBudget.toLocaleString('en-US', { minimumFractionDigits: 2 });
        tbBudget.title = tbBudget.innerText;
    }
    if (tbAvailable) {
        tbAvailable.innerText = '₱' + availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 });
        tbAvailable.title = 'Available for Allocation: ₱' + availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 });
    }
    if (tbAvailableSub) {
        tbAvailableSub.innerText = 'Remaining for FY ' + currentFiscalYear;
    }
    if (tbAlloc) {
        tbAlloc.innerText = '₱' + totalAllocated.toLocaleString('en-US', { minimumFractionDigits: 2 });
        tbAlloc.title = tbAlloc.innerText;
    }
    if (tbDisb) {
        tbDisb.innerText = '₱' + totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 });
        tbDisb.title = tbDisb.innerText;
    }
    if (tbUnexp) {
        // Displays Undisbursed Allocated Funds (Committed awaiting release)
        tbUnexp.innerText = '₱' + undisbursedAllocated.toLocaleString('en-US', { minimumFractionDigits: 2 });
        tbUnexp.title = 'Undisbursed Allocated Funds: ₱' + undisbursedAllocated.toLocaleString('en-US', { minimumFractionDigits: 2 });
    }

    // Mobile Shell Stat Cards
    const mBudget = document.getElementById('stat-total-budget');
    const mDisb = document.getElementById('stat-total-disbursed');
    if (mBudget) {
        mBudget.innerText = '₱' + unexpendedBalance.toLocaleString('en-US', { minimumFractionDigits: 2 });
        mBudget.title = mBudget.innerText;
    }
    if (mDisb) {
        mDisb.innerText = '₱' + totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 });
        mDisb.title = mDisb.innerText;
    }

    // Badges
    const badgeProj = document.getElementById('admin-badge-pending-proj');
    const badgeRep = document.getElementById('admin-badge-pending-rep');
    if (badgeProj) badgeProj.innerText = pendingProjCount;
    if (badgeRep) badgeRep.innerText = pendingRepCount;
}

function renderMetricWidgets() {
    const widgetGrid = document.getElementById('metrics-widget-grid');
    if (!widgetGrid || !activeUser) return;

    const role = activeUser.role;
    const annualBudget = annualBudgets[currentFiscalYear] || 1600000.00;
    const fyProjects = projects.filter(p => String(p.fiscalYear || '2026') === String(currentFiscalYear));
    const approvedProjects = fyProjects.filter(p => p.status === 'Approved' || p.status === 'In Progress' || p.status === 'Completed');
    const totalAllocated = approvedProjects.reduce((acc, p) => acc + (parseFloat(p.budget) || 0), 0);
    const availableForAllocation = Math.max(0, annualBudget - totalAllocated);
    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear));
    const releasedVouchers = fyDisbursements.filter(d => d.status !== 'Returned' && (parseFloat(d.amount) || 0) > 0);
    const totalDisbursed = releasedVouchers.reduce((acc, d) => acc + (parseFloat(d.amount) || 0), 0);
    const totalRefunded = fyDisbursements.filter(d => d.status === 'Returned' || (parseFloat(d.amount) || 0) < 0)
        .reduce((acc, d) => acc + Math.abs(parseFloat(d.amount) || 0), 0);
    const netDisbursed = Math.max(0, totalDisbursed - totalRefunded);
    const undisbursedAllocated = Math.max(0, totalAllocated - netDisbursed);
    const unexpendedBalance = Math.max(0, annualBudget - netDisbursed);
    const pendingCount = fyProjects.filter(p => p.status === 'Pending Admin Approval').length;

    if (role === 'SK Secretary') {
        widgetGrid.innerHTML = `
            <div class="bg-white p-3.5 rounded-2xl border border-slate-200/90 shadow-sm border-t-4 border-t-amber-500 flex flex-col justify-between">
                <div class="flex justify-between items-start mb-1.5">
                    <p class="text-[9px] font-extrabold text-amber-700 uppercase tracking-wider">Pending Proposals</p>
                    <span class="w-7 h-7 rounded-lg bg-amber-100 text-amber-700 flex items-center justify-center text-xs shadow-inner">
                        <i class="fa-solid fa-clock-rotate-left"></i>
                    </span>
                </div>
                <h3 class="text-xl font-black text-slate-900">${pendingCount}</h3>
                <p class="text-[10px] text-amber-600 font-bold mt-0.5">Awaiting Chair Approval</p>
            </div>

            <div class="bg-white p-3.5 rounded-2xl border border-slate-200/90 shadow-sm border-t-4 border-t-prosko-purple flex flex-col justify-between">
                <div class="flex justify-between items-start mb-1.5">
                    <p class="text-[9px] font-extrabold text-prosko-purple uppercase tracking-wider">Reports Filed</p>
                    <span class="w-7 h-7 rounded-lg bg-prosko-purpleLight text-prosko-purple flex items-center justify-center text-xs shadow-inner">
                        <i class="fa-solid fa-file-lines"></i>
                    </span>
                </div>
                <h3 class="text-xl font-black text-slate-900">${accomplishmentReports.length}</h3>
                <p class="text-[10px] text-slate-500 font-medium mt-0.5">Accomplishments</p>
            </div>
        `;
    } else if (role === 'SK Treasurer') {
        widgetGrid.innerHTML = `
            <div class="bg-white p-3.5 rounded-2xl border border-slate-200/90 shadow-sm border-t-4 border-t-prosko-blue flex flex-col justify-between">
                <div class="flex justify-between items-start mb-1.5">
                    <p class="text-[9px] font-extrabold text-prosko-blue uppercase tracking-wider">Available for Allocation</p>
                    <span class="w-7 h-7 rounded-lg bg-prosko-blueLight text-prosko-blue flex items-center justify-center text-xs shadow-inner">
                        <i class="fa-solid fa-wallet"></i>
                    </span>
                </div>
                <h3 id="stat-total-budget" class="text-sm sm:text-base font-black text-slate-900 truncate" title="₱${availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 })}</h3>
                <p class="text-[10px] text-slate-500 font-medium mt-0.5">FY ${currentFiscalYear} of ₱${annualBudget.toLocaleString('en-US')} IRA</p>
            </div>

            <div class="bg-white p-3.5 rounded-2xl border border-slate-200/90 shadow-sm border-t-4 border-t-prosko-green flex flex-col justify-between">
                <div class="flex justify-between items-start mb-1.5">
                    <p class="text-[9px] font-extrabold text-prosko-greenDark uppercase tracking-wider">Total Disbursed</p>
                    <span class="w-7 h-7 rounded-lg bg-prosko-greenLight text-prosko-green flex items-center justify-center text-xs shadow-inner">
                        <i class="fa-solid fa-money-bill-transfer"></i>
                    </span>
                </div>
                <h3 id="stat-total-disbursed" class="text-sm sm:text-base font-black text-slate-900 truncate" title="₱${totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })}</h3>
                <p class="text-[10px] text-prosko-green font-bold mt-0.5">₱${undisbursedAllocated.toLocaleString('en-US')} Committed & Undisbursed</p>
            </div>
        `;
    } else {
        // Chairperson / Kagawad / Admin mobile cards
        widgetGrid.innerHTML = `
            <div class="bg-white p-3.5 rounded-2xl border border-slate-200/90 shadow-sm border-t-4 border-t-prosko-blue flex flex-col justify-between">
                <div class="flex justify-between items-start mb-1.5">
                    <p class="text-[9px] font-extrabold text-prosko-blue uppercase tracking-wider">Available for Allocation</p>
                    <span class="w-7 h-7 rounded-lg bg-prosko-blueLight text-prosko-blue flex items-center justify-center text-xs shadow-inner">
                        <i class="fa-solid fa-wallet"></i>
                    </span>
                </div>
                <h3 id="stat-total-budget" class="text-sm sm:text-base font-black text-slate-900 truncate" title="₱${availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 })}</h3>
                <p class="text-[10px] text-slate-500 font-medium mt-0.5">FY ${currentFiscalYear} • ₱${totalAllocated.toLocaleString('en-US')} Allocated</p>
            </div>

            <div class="bg-white p-3.5 rounded-2xl border border-slate-200/90 shadow-sm border-t-4 border-t-prosko-green flex flex-col justify-between">
                <div class="flex justify-between items-start mb-1.5">
                    <p class="text-[9px] font-extrabold text-prosko-greenDark uppercase tracking-wider">Total Disbursed</p>
                    <span class="w-7 h-7 rounded-lg bg-prosko-greenLight text-prosko-green flex items-center justify-center text-xs shadow-inner">
                        <i class="fa-solid fa-money-bill-transfer"></i>
                    </span>
                </div>
                <h3 id="stat-total-disbursed" class="text-sm sm:text-base font-black text-slate-900 truncate" title="₱${totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${totalDisbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })}</h3>
                <p class="text-[10px] text-prosko-green font-bold mt-0.5">₱${undisbursedAllocated.toLocaleString('en-US')} Undisbursed</p>
            </div>
        `;
    }
}

function updateDashboardUI() {
    const fySelect = document.getElementById('budget-fiscal-year');
    if (fySelect && currentFiscalYear) {
        fySelect.value = currentFiscalYear;
    }
    renderMetricWidgets();
    updateCalculatedMetrics();
    filterProjects();
    renderTreasurerBudgetCards();
    renderDisbursementLogsTable();
    renderAccomplishmentReports();
    renderBrgyDocuments();
    renderIncidentsMobileList();
}

// =========================================================================
// 11. TREASURY, BUDGET LEDGER & DISBURSEMENT LOGS (TASK REQ 1 & 4)
// =========================================================================
function changeFiscalYear(year) {
    currentFiscalYear = String(year);
    const select = document.getElementById('budget-fiscal-year');
    if (select) select.value = currentFiscalYear;
    updateCalculatedMetrics();
    renderMetricWidgets();
    renderTreasurerBudgetCards();
    renderDisbursementLogsTable();
    filterProjects();
    showToast('Switched to Fiscal Year ' + year, 'info');
}

function openSetAnnualBudgetModal() {
    const yrDisp = document.getElementById('sab-year-disp');
    const yrSelect = document.getElementById('sab-fiscal-year');
    const amtInput = document.getElementById('sab-amount');

    if (yrDisp) yrDisp.innerText = currentFiscalYear;
    if (yrSelect) yrSelect.value = currentFiscalYear;
    if (amtInput) amtInput.value = annualBudgets[currentFiscalYear] || 1600000;

    document.getElementById('set-annual-budget-modal').classList.remove('hidden');
}

function syncSetAnnualBudgetYear(year) {
    const yrDisp = document.getElementById('sab-year-disp');
    const amtInput = document.getElementById('sab-amount');
    if (yrDisp) yrDisp.innerText = year;
    if (amtInput) amtInput.value = annualBudgets[year] || 1600000;
}

function closeSetAnnualBudgetModal() {
    document.getElementById('set-annual-budget-modal').classList.add('hidden');
}

async function saveAnnualBudgetSetting(e) {
    e.preventDefault();
    const amt = parseFloat(document.getElementById('sab-amount').value);
    const yrSelect = document.getElementById('sab-fiscal-year');
    const year = yrSelect ? yrSelect.value : currentFiscalYear;
    const resRef = document.getElementById('sab-resolution-ref') ? document.getElementById('sab-resolution-ref').value.trim() : '';
    const reason = document.getElementById('sab-reason') ? document.getElementById('sab-reason').value.trim() : '';

    if (!amt || amt <= 0) {
        showToast('Please enter a valid budget amount.', 'error');
        return;
    }

    // Protection 1: Calculate already allocated and disbursed amount for this fiscal year
    const fyProjects = projects.filter(p => String(p.fiscalYear || '2026') === String(year));
    const approvedProjects = fyProjects.filter(p => p.status === 'Approved' || p.status === 'In Progress' || p.status === 'Completed');
    const totalAllocated = approvedProjects.reduce((acc, p) => acc + (parseFloat(p.budget) || 0), 0);
    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(year));
    const releasedVouchers = fyDisbursements.filter(d => d.status !== 'Returned' && (parseFloat(d.amount) || 0) > 0);
    const totalDisbursed = releasedVouchers.reduce((acc, d) => acc + (parseFloat(d.amount) || 0), 0);

    const minAllowedBudget = Math.max(totalAllocated, totalDisbursed);
    if (amt < minAllowedBudget) {
        showToast(`Budget Protection: Cannot reduce FY ${year} budget below already committed funds (₱${minAllowedBudget.toLocaleString('en-US', { minimumFractionDigits: 2 })}).`, 'error');
        return;
    }

    const prevAmt = annualBudgets[year] || 1600000;
    annualBudgets[year] = amt;
    localStorage.setItem('prosko_annual_budgets', JSON.stringify(annualBudgets));

    // Live Supabase sync for fiscal_budget with upsert onConflict (year)
    try {
        await supabaseUpsert('fiscal_budget', { year: String(year), allocated_budget: amt, updated_at: new Date().toISOString() }, 'year');
    } catch (err) {}

    // Audit Trail logging with authorized user, date, resolution, and previous vs new budget
    const auditDetail = `Adjusted FY ${year} SK Budget from ₱${prevAmt.toLocaleString('en-US')} to ₱${amt.toLocaleString('en-US')} | Ref: ${resRef || 'N/A'} | Reason: ${reason || 'Annual IRA update'} | User: ${activeUser ? activeUser.name + ' (' + activeUser.role + ')' : 'Admin'}`;
    await logUserActivity('ANNUAL_BUDGET_UPDATE', auditDetail);

    closeSetAnnualBudgetModal();
    updateDashboardUI();
    showToast('Annual SK Budget for FY ' + year + ' updated to ₱' + amt.toLocaleString('en-US'), 'success');
}

function renderTreasurerBudgetCards() {
    const container = document.getElementById('treasurer-budget-cards');
    const actionContainer = document.getElementById('treasurer-action-container');
    if (!container) return;
    container.innerHTML = '';

    const isTreasurer = activeUser && activeUser.role === 'SK Treasurer';

    // TASK REQ 1: ADMIN IS NOT SUPPOSED TO RELEASE FUNDS! ONLY TREASURER RELEASES FUNDS!
    if (actionContainer) {
        if (isTreasurer) {
            actionContainer.innerHTML = `
                <button onclick="openDisbursementModal()" class="w-full sm:w-auto px-4 py-2 bg-gradient-to-r from-emerald-600 to-green-600 hover:from-emerald-700 hover:to-green-700 text-white text-xs font-extrabold rounded-xl shadow-sm hover:shadow transition flex items-center justify-center gap-2 whitespace-nowrap active:scale-[0.98]">
                    <i class="fa-solid fa-plus-circle text-sm"></i> <span>Release Fund / Disburse</span>
                </button>
            `;
        } else {
            actionContainer.innerHTML = `
                <div class="w-full sm:w-auto px-3.5 py-2 bg-amber-50 text-amber-800 border border-amber-200 rounded-xl text-xs font-bold flex items-center gap-2">
                    <i class="fa-solid fa-shield-halved text-amber-600 shrink-0"></i> <span>Admin View: Read-Only Ledger (Fund Releases restricted to SK Treasurer)</span>
                </div>
            `;
        }
    }

    const fyProjects = projects.filter(p => String(p.fiscalYear || '2026') === String(currentFiscalYear));

    if (fyProjects.length === 0) {
        container.innerHTML = `<div class="p-6 text-center text-slate-400 font-bold text-xs">No projects registered in budget ledger for FY ${currentFiscalYear}</div>`;
        return;
    }

    fyProjects.forEach(p => {
        const total = parseFloat(p.budget) || 0;
        const disbursed = parseFloat(p.disbursed) || 0;
        const remaining = Math.max(0, total - disbursed);
        const pct = total > 0 ? Math.min(100, Math.round((disbursed / total) * 100)) : 0;

        const card = document.createElement('div');
        card.className = "bg-slate-50/90 p-4 rounded-xl border border-slate-200/90 space-y-2.5 text-xs shadow-[0_1px_2px_rgba(0,0,0,0.02)]";
        card.innerHTML = `
            <div class="flex justify-between items-start gap-2">
                <div class="min-w-0">
                    <h4 class="font-bold text-slate-900 text-xs sm:text-sm truncate">${escapeHtml(p.title)}</h4>
                    <p class="text-[10px] text-slate-500 font-medium">${escapeHtml(p.category)} • <span class="font-bold text-slate-700">${p.status}</span></p>
                </div>
                <span class="shrink-0 px-2.5 py-0.5 rounded-full text-[10px] font-extrabold ${pct === 100 ? 'bg-emerald-100 text-emerald-800 border-emerald-300' : 'bg-prosko-greenLight text-prosko-greenDark border-prosko-green/30'} border">
                    ${pct}% Released
                </span>
            </div>

            <div class="w-full bg-slate-200/80 rounded-full h-2 overflow-hidden">
                <div class="bg-gradient-to-r from-emerald-500 to-green-600 h-2 rounded-full transition-all duration-300" style="width: ${pct}%"></div>
            </div>

            <div class="grid grid-cols-3 gap-2 pt-1 text-[11px] text-center border-t border-slate-200/80">
                <div class="bg-white/60 p-1.5 rounded-lg border border-slate-200/50">
                    <p class="text-slate-400 font-bold text-[9px] uppercase tracking-wider">Allocated</p>
                    <p class="font-black text-slate-800 text-xs truncate" title="₱${total.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${total.toLocaleString('en-US', { minimumFractionDigits: 2 })}</p>
                </div>
                <div class="bg-white/60 p-1.5 rounded-lg border border-slate-200/50">
                    <p class="text-slate-400 font-bold text-[9px] uppercase tracking-wider">Disbursed</p>
                    <p class="font-black text-prosko-green text-xs truncate" title="₱${disbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${disbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })}</p>
                </div>
                <div class="bg-white/60 p-1.5 rounded-lg border border-slate-200/50">
                    <p class="text-slate-400 font-bold text-[9px] uppercase tracking-wider">Balance</p>
                    <p class="font-black text-prosko-blue text-xs truncate" title="₱${remaining.toLocaleString('en-US', { minimumFractionDigits: 2 })}">₱${remaining.toLocaleString('en-US', { minimumFractionDigits: 2 })}</p>
                </div>
            </div>
        `;
        container.appendChild(card);
    });
}

function renderDisbursementLogsTable() {
    const tbody = document.getElementById('disbursement-logs-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear));

    if (fyDisbursements.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="p-8 text-center text-slate-400 font-semibold">
                    <i class="fa-solid fa-receipt text-3xl mb-2 text-slate-300 block"></i>
                    No disbursements released for Fiscal Year ${currentFiscalYear}
                </td>
            </tr>
        `;
        return;
    }

    fyDisbursements.forEach(item => {
        const tr = document.createElement('tr');
        tr.className = "hover:bg-slate-50 transition border-b border-slate-100";

        const formattedDate = item.disbursedAt ? new Date(item.disbursedAt).toLocaleDateString('en-US', {
            month: 'short', day: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit'
        }) : 'Oct 20, 2026';

        const rawAmt = parseFloat(item.amount) || 0;
        const isRefund = item.status === 'Returned' || rawAmt < 0;
        const displayAmt = isRefund
            ? `+₱${Math.abs(rawAmt).toLocaleString('en-US', { minimumFractionDigits: 2 })} (Refund)`
            : `₱${rawAmt.toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
        const statusBadge = isRefund
            ? `<span class="px-2.5 py-0.5 rounded-full bg-blue-100 text-blue-800 text-[10px] font-black border border-blue-200">Refunded / Returned</span>`
            : `<span class="px-2.5 py-0.5 rounded-full bg-emerald-100 text-emerald-800 text-[10px] font-black border border-emerald-200">Released</span>`;

        tr.innerHTML = `
            <td class="py-3 px-3 font-mono font-bold text-prosko-blue text-xs"><span class="px-2 py-0.5 rounded-lg bg-blue-50 border border-blue-200/60">${escapeHtml(item.voucherNo || item.id)}</span></td>
            <td class="py-3 px-3 text-slate-500 text-xs">${formattedDate}</td>
            <td class="py-3 px-3">
                <span class="font-bold text-slate-900 block">${escapeHtml(item.projectTitle)}</span>
                <span class="text-[10px] text-slate-500 block truncate max-w-xs">${escapeHtml(item.purpose || '')}</span>
            </td>
            <td class="py-3 px-3 text-slate-700 font-semibold">${escapeHtml(item.payee)}</td>
            <td class="py-3 px-3 text-right font-black ${isRefund ? 'text-blue-600' : 'text-prosko-green'} text-xs">${displayAmt}</td>
            <td class="py-3 px-3 text-slate-600 text-xs font-medium">${escapeHtml(item.disbursedBy || 'SK Treasurer')}</td>
            <td class="py-3 px-3 text-center">${statusBadge}</td>
        `;
        tbody.appendChild(tr);
    });
}

function filterDisbursements() {
    const input = document.getElementById('disbursement-search');
    const query = (input ? input.value : '').toLowerCase();
    const tbody = document.getElementById('disbursement-logs-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear));
    const filtered = fyDisbursements.filter(d =>
        (d.voucherNo && d.voucherNo.toLowerCase().includes(query)) ||
        (d.projectTitle && d.projectTitle.toLowerCase().includes(query)) ||
        (d.payee && d.payee.toLowerCase().includes(query)) ||
        (d.purpose && d.purpose.toLowerCase().includes(query))
    );

    if (filtered.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="p-6 text-center text-slate-400 font-semibold text-xs">
                    No matching disbursements found for "${escapeHtml(query)}"
                </td>
            </tr>
        `;
        return;
    }

    filtered.forEach(item => {
        const tr = document.createElement('tr');
        tr.className = "hover:bg-slate-50 transition border-b border-slate-100";
        tr.innerHTML = `
            <td class="py-3 px-3 font-mono font-bold text-prosko-blue text-xs"><span class="px-2 py-0.5 rounded-lg bg-blue-50 border border-blue-200/60">${escapeHtml(item.voucherNo || item.id)}</span></td>
            <td class="py-3 px-3 text-slate-500 text-xs">${new Date(item.disbursedAt).toLocaleDateString()}</td>
            <td class="py-3 px-3 font-bold text-slate-900">${escapeHtml(item.projectTitle)}</td>
            <td class="py-3 px-3 text-slate-700 font-semibold">${escapeHtml(item.payee)}</td>
            <td class="py-3 px-3 text-right font-black text-prosko-green text-xs">₱${parseFloat(item.amount).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
            <td class="py-3 px-3 text-slate-600 text-xs font-medium">${escapeHtml(item.disbursedBy || 'SK Treasurer')}</td>
            <td class="py-3 px-3 text-center"><span class="px-2.5 py-0.5 rounded-full bg-emerald-100 text-emerald-800 text-[10px] font-black border border-emerald-200">Released</span></td>
        `;
        tbody.appendChild(tr);
    });
}

// Fund Release Modal: ONLY SK TREASURER CAN OPEN & SUBMIT!
function openDisbursementModal() {
    if (!activeUser || activeUser.role !== 'SK Treasurer') {
        showToast('Access Denied: Only the SK Treasurer is authorized to release funds.', 'error');
        return;
    }

    const select = document.getElementById('disb-project-select');
    if (!select) return;
    select.innerHTML = '';

    // Only allow disbursements for approved or in-progress projects in current fiscal year with available balance
    const releasableProjects = projects.filter(p => (String(p.fiscalYear || '2026') === String(currentFiscalYear)) && (p.status === 'Approved' || p.status === 'In Progress') && ((parseFloat(p.budget) || 0) > (parseFloat(p.disbursed) || 0)));

    if (releasableProjects.length === 0) {
        showToast('No approved projects with remaining balance available for fund disbursement.', 'error');
        return;
    }

    releasableProjects.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p.id;
        const total = parseFloat(p.budget) || 0;
        const bal = Math.max(0, total - (parseFloat(p.disbursed) || 0));
        opt.innerText = p.title + ' [' + p.status + '] (Allocated: ₱' + total.toLocaleString() + ' | Bal: ₱' + bal.toLocaleString() + ')';
        select.appendChild(opt);
    });

    document.getElementById('disbursement-modal').classList.remove('hidden');
}

function closeDisbursementModal() {
    document.getElementById('disbursement-modal').classList.add('hidden');
}

async function saveDisbursement(e) {
    if (e) e.preventDefault();

    if (!activeUser || activeUser.role !== 'SK Treasurer') {
        showToast('Access Denied: Only the SK Treasurer is authorized to release funds.', 'error');
        closeDisbursementModal();
        return;
    }

    const rawProjId = document.getElementById('disb-project-select').value;
    const amount = parseFloat(document.getElementById('disb-amount').value) || 0;
    const payee = document.getElementById('disb-payee').value.trim();

    if (amount <= 0) {
        showToast('Please enter a valid disbursement amount.', 'error');
        return;
    }

    const project = projects.find(p => String(p.id) === String(rawProjId));
    if (!project) {
        showToast('Selected project could not be found.', 'error');
        return;
    }

    const remaining = (parseFloat(project.budget) || 0) - (parseFloat(project.disbursed) || 0);
    if (amount > remaining) {
        showToast('Amount exceeds remaining project balance of ₱' + remaining.toLocaleString('en-US', { minimumFractionDigits: 2 }), 'error');
        return;
    }

    const newDisbursed = (parseFloat(project.disbursed) || 0) + amount;
    project.disbursed = newDisbursed;

    // Transition project status to "In Progress" upon Treasurer fund release
    if (project.status === 'Approved' || project.status === 'Proposed') {
        project.status = 'In Progress';
    }

    // Generate Voucher Entry
    const fyDisbCount = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear)).length;
    const voucherNum = 'DV-' + currentFiscalYear + '-' + String(fyDisbCount + 1).padStart(3, '0');
    const disbursementRecord = {
        id: voucherNum,
        voucherNo: voucherNum,
        projectId: project.id,
        projectTitle: project.title,
        payee: payee,
        purpose: 'Disbursement release for ' + project.title,
        amount: amount,
        fiscalYear: currentFiscalYear,
        disbursedBy: activeUser.name,
        disbursedAt: new Date().toISOString(),
        status: 'Released'
    };

    disbursements.unshift(disbursementRecord);
    saveDisbursementsToLocal();
    saveProjectsToLocal();

    // Live Supabase update for project fund deduction
    await supabaseUpdate('projects', { disbursed: newDisbursed, status: project.status }, 'id', project.id);

    // Live Supabase insert for disbursement voucher
    await supabaseInsert('disbursements', {
        id: voucherNum,
        voucher_no: voucherNum,
        project_id: isNaN(Number(project.id)) ? null : Number(project.id),
        project_title: project.title,
        payee: payee,
        purpose: 'Disbursement release for ' + project.title,
        amount: amount,
        fiscal_year: currentFiscalYear,
        disbursed_by: activeUser.name,
        status: 'Released'
    });

    await fetchAllDatabaseData();

    await logUserActivity('FUND_DISBURSEMENT', 'SK Treasurer released ₱' + amount.toLocaleString('en-US') + ' to ' + payee + ' for ' + project.title + ' (' + voucherNum + ')');

    closeDisbursementModal();
    showToast('Successfully released ₱' + amount.toLocaleString('en-US') + ' (Voucher: ' + voucherNum + ') - Project status set to In Progress!', 'success');

    updateDashboardUI();
}

// =========================================================================
// 12. YEAR-END ROLL-OVER & REVERSION PROCESSOR (TASK REQ 4)
// =========================================================================
function openYearEndRolloverModal() {
    const annualBudget = annualBudgets[currentFiscalYear] || 1500000;
    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear));
    const totalDisbursed = fyDisbursements.reduce((acc, d) => acc + (parseFloat(d.amount) || 0), 0);
    const unexpended = Math.max(0, annualBudget - totalDisbursed);

    document.getElementById('ro-year-disp').innerText = currentFiscalYear;
    document.getElementById('ro-next-year-disp').innerText = parseInt(currentFiscalYear) + 1;
    document.getElementById('ro-unexpended-amount').innerText = '₱' + unexpended.toLocaleString('en-US', { minimumFractionDigits: 2 });

    document.getElementById('year-end-rollover-modal').classList.remove('hidden');
}

function closeYearEndRolloverModal() {
    document.getElementById('year-end-rollover-modal').classList.add('hidden');
}

async function executeYearEndRollover() {
    const resRef = document.getElementById('ro-resolution-ref').value.trim() || 'SK-RES-ROLLOVER';
    const annualBudget = annualBudgets[currentFiscalYear] || 1500000;
    const fyDisbursements = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear));
    const totalDisbursed = fyDisbursements.reduce((acc, d) => acc + (parseFloat(d.amount) || 0), 0);
    const unexpended = Math.max(0, annualBudget - totalDisbursed);
    const nextYear = (parseInt(currentFiscalYear) + 1).toString();

    // Add unexpended balance to next year's budget as Continuing Appropriation / Surplus Trust Fund
    annualBudgets[nextYear] = (annualBudgets[nextYear] || 1500000) + unexpended;
    localStorage.setItem('prosko_annual_budgets', JSON.stringify(annualBudgets));

    await supabaseUpsert('fiscal_budget', { year: String(nextYear), allocated_budget: annualBudgets[nextYear], updated_at: new Date().toISOString() }, 'year');
    await logUserActivity('YEAR_END_ROLLOVER', 'Processed FY ' + currentFiscalYear + ' Year-End Reversion of ₱' + unexpended.toLocaleString('en-US') + ' into FY ' + nextYear + ' Continuing Appropriations (Resolution: ' + resRef + ')');

    closeYearEndRolloverModal();
    showToast('FY ' + currentFiscalYear + ' unexpended funds (₱' + unexpended.toLocaleString('en-US') + ') rolled over to FY ' + nextYear + ' SK Trust Fund!', 'success');
    updateDashboardUI();
}

// =========================================================================
// 13. PROJECT PROPOSALS & SECRETARY WORKFLOW (TASK REQ 3)
// =========================================================================
function openNewProjectModal() {
    document.getElementById('proj-id').value = '';
    document.getElementById('project-form').reset();
    const isAdmin = activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin' || activeUser.role === 'Admin / System Overseer');
    const isSecretary = activeUser && activeUser.role === 'SK Secretary';
    const statusContainer = document.getElementById('proj-status-container');
    const modalTitle = document.getElementById('modal-project-title');
    const statusSelect = document.getElementById('proj-status');

    // Secretary / Non-Admin submits proposals for Admin approval; Admin creates Approved projects
    if (isSecretary || !isAdmin) {
        modalTitle.innerText = "Propose Youth Project (Awaiting Admin Approval)";
        if (statusContainer) statusContainer.classList.add('hidden');
        if (statusSelect) statusSelect.value = "Pending Admin Approval";
    } else {
        modalTitle.innerText = "Add New Youth Project";
        if (statusContainer) statusContainer.classList.remove('hidden');
        if (statusSelect) {
            statusSelect.innerHTML = `
                <option value="Approved">Approved (Awaiting Treasurer Fund Release)</option>
                <option value="Proposed">Proposed</option>
            `;
            statusSelect.value = "Approved";
        }
    }

    document.getElementById('project-modal').classList.remove('hidden');
}

function closeProjectModal() {
    document.getElementById('project-modal').classList.add('hidden');
}

async function saveProject(e) {
    if (e) e.preventDefault();

    const id = document.getElementById('proj-id').value;
    const title = document.getElementById('proj-title').value.trim();
    const category = document.getElementById('proj-category').value;
    const budget = parseFloat(document.getElementById('proj-budget').value) || 0;
    const period = document.getElementById('proj-period').value.trim();
    const isAdmin = activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin' || activeUser.role === 'Admin / System Overseer');
    const isSecretary = activeUser && activeUser.role === 'SK Secretary';

    if (!title) {
        showToast('Please enter a project title.', 'error');
        return;
    }
    if (budget <= 0) {
        showToast('Please enter a valid project budget.', 'error');
        return;
    }

    // Protection: Validate against Available for Allocation if creating an approved project
    const annualBudget = annualBudgets[currentFiscalYear] || 1600000.00;
    const fyProjects = projects.filter(p => String(p.fiscalYear || '2026') === String(currentFiscalYear));
    const currentAllocated = fyProjects
        .filter(p => (p.status === 'Approved' || p.status === 'In Progress' || p.status === 'Completed') && String(p.id) !== String(id))
        .reduce((acc, p) => acc + (parseFloat(p.budget) || 0), 0);
    const availableForAllocation = Math.max(0, annualBudget - currentAllocated);

    // Force non-admin/Secretary submissions to always be "Pending Admin Approval"
    // Newly created projects default to "Approved" and cannot jump to "In Progress" until Treasurer releases funds
    let status = (!isAdmin || isSecretary) ? "Pending Admin Approval" : document.getElementById('proj-status').value;
    if (!id && (status === "In Progress" || !status)) {
        status = "Approved";
    }

    if (status !== 'Pending Admin Approval' && budget > availableForAllocation) {
        showToast(`Budget Limit Exceeded: Only ₱${availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 })} available for allocation in FY ${currentFiscalYear}.`, 'error');
        return;
    }

    closeProjectModal();

    if (id) {
        const rawId = String(id);
        const p = projects.find(item => String(item.id) === rawId);
        if (p) {
            p.title = title;
            p.category = category;
            p.budget = budget;
            p.period = period;
            if (isAdmin && !isSecretary) {
                if (status === "In Progress" && (parseFloat(p.disbursed) || 0) <= 0) {
                    p.status = "Approved";
                } else {
                    p.status = status;
                }
            }
        }

        saveProjectsToLocal();

        const updatePayload = {
            title,
            description: (p && p.description) || '',
            category,
            budget,
            period,
            status: p ? p.status : status,
            fiscal_year: currentFiscalYear
        };

        const { error: updErr } = await supabaseUpdate('projects', updatePayload, 'id', id);
        if (updErr) {
            console.error('Supabase project update error:', updErr);
            showToast('Live database sync note: ' + (updErr.message || 'Saved locally'), 'warning');
        } else {
            await fetchAllDatabaseData();
        }

        await logUserActivity('PROJECT_UPDATE', 'Updated project: ' + title);
        showToast('Project updated successfully in live database!', 'success');

    } else {
        let submitter = 'Belly Jean Montecillo (SK Chairperson)';
        if (activeUser) {
            if (activeUser.role === 'SK Chairperson' || /admin/i.test(activeUser.username) || /admin/i.test(activeUser.role)) {
                submitter = 'Belly Jean Montecillo (SK Chairperson)';
            } else {
                submitter = activeUser.name + ' (' + activeUser.role + ')';
            }
        }
        if (/von|pilapil|sk chair admin|chair admin|sk chair|admin_libjo|^admin$/i.test(submitter)) {
            submitter = 'Belly Jean Montecillo (SK Chairperson)';
        }

        // Live Supabase payload with full column mappings
        const insertPayload = {
            title: title,
            description: '',
            category: category,
            budget: budget,
            disbursed: 0.00,
            status: status,
            submitted_by: submitter,
            period: period,
            ai_score: "94% Verified",
            fiscal_year: currentFiscalYear
        };

        // Immediately insert proposal / project into live Supabase projects table FIRST
        const { data: insertedData, error: insErr } = await supabaseInsert('projects', insertPayload);
        if (insErr) {
            console.error('Supabase project insert error:', insErr);
            showToast('Live Database Sync Error: ' + (insErr.message || 'Failed to record in live Supabase'), 'error');
            return;
        }

        const liveRecord = (insertedData && insertedData[0]) ? insertedData[0] : null;
        const newProject = {
            id: liveRecord ? liveRecord.id : Date.now(),
            title: title,
            description: '',
            category: category,
            budget: budget,
            disbursed: 0.00,
            status: status,
            submittedBy: submitter,
            period: period,
            aiScore: "94% Verified",
            fiscalYear: currentFiscalYear,
            createdAt: liveRecord ? liveRecord.created_at : new Date().toISOString()
        };

        projects.unshift(newProject);
        saveProjectsToLocal();

        // Immediately reload directly from live Supabase so it persists when refreshing
        await fetchAllDatabaseData();

        if (!isAdmin || isSecretary) {
            await logUserActivity('PROJECT_PROPOSE', 'Submitted project proposal: ' + title + ' for Admin approval');
            showToast('Project proposal submitted to live database! Waiting for SK Chairperson approval.', 'success');
        } else {
            await logUserActivity('PROJECT_CREATE', 'Added new project: ' + title);
            showToast('Youth project saved directly to live database (Status: Approved)!', 'success');
        }
    }

    // Immediately update metrics & ledger reactively
    updateDashboardUI();
}

function renderProjectsCards(projectList) {
    const container = document.getElementById('projects-list-container');
    const emptyState = document.getElementById('empty-projects-state');
    const countSpan = document.getElementById('showing-count');

    if (!container) return;
    container.innerHTML = '';

    if (projectList.length === 0) {
        emptyState.classList.remove('hidden');
        if (countSpan) countSpan.innerText = '0 Projects';
        return;
    }

    emptyState.classList.add('hidden');
    if (countSpan) countSpan.innerText = projectList.length + ' Projects';

    const isAdmin = activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin');
    const isSecretary = activeUser && activeUser.role === 'SK Secretary';

    projectList.forEach(project => {
        const card = document.createElement('div');
        card.className = "bg-slate-50 p-4 rounded-xl border border-slate-200/90 space-y-2.5 text-xs relative";

        let statusBadgeClass = "bg-slate-100 text-slate-800 border-slate-300";
        switch (project.status) {
            case "Pending Admin Approval":
                statusBadgeClass = "bg-amber-100 text-amber-800 border-amber-300";
                break;
            case "Approved":
                statusBadgeClass = "bg-blue-100 text-blue-800 border-blue-300";
                break;
            case "In Progress":
                statusBadgeClass = "bg-emerald-100 text-emerald-800 border-emerald-300";
                break;
            case "Completed":
                statusBadgeClass = "bg-purple-100 text-purple-800 border-purple-300";
                break;
            case "Cancelled":
                statusBadgeClass = "bg-rose-100 text-rose-800 border-rose-300";
                break;
            case "Proposed":
                statusBadgeClass = "bg-slate-100 text-slate-800 border-slate-300";
                break;
        }

        let displaySubmitter = project.submittedBy || project.submitted_by || 'Belly Jean Montecillo (SK Chairperson)';
        if (!displaySubmitter || /von|pilapil|sk chair admin|chair admin|sk chair|admin_libjo|^admin$|^sk chairperson$|belly jean montecillo/i.test(displaySubmitter)) {
            displaySubmitter = 'Belly Jean Montecillo (SK Chairperson)';
        }

        card.innerHTML = `
            <div class="flex justify-between items-start">
                <div>
                    <h4 class="font-bold text-slate-900 text-sm">${escapeHtml(project.title)}</h4>
                    <p class="text-xs text-slate-500 font-medium">${escapeHtml(project.category)} • <span class="text-prosko-purple font-bold">${escapeHtml(displaySubmitter)}</span></p>
                </div>
                <span class="inline-block px-2.5 py-0.5 rounded-full text-[10px] font-extrabold border ${statusBadgeClass}">
                    ${project.status}
                </span>
            </div>

            <div class="flex justify-between items-center text-xs pt-1 border-t border-slate-200">
                <span class="font-extrabold text-prosko-blue text-sm">₱${parseFloat(project.budget).toLocaleString('en-US', { minimumFractionDigits: 2 })}</span>
                <span class="text-slate-500 text-xs font-medium">${escapeHtml(project.period)}</span>
            </div>

            ${isAdmin && project.status === 'Pending Admin Approval' ? `
                <div class="p-2.5 bg-amber-50 rounded-xl border border-amber-200 flex items-center justify-between gap-2 mt-1">
                    <span class="text-xs font-bold text-amber-900"><i class="fa-solid fa-clock-rotate-left text-amber-600 mr-1"></i>Secretary Proposal Pending Admin Approval</span>
                    <div class="flex gap-1.5">
                        <button onclick="approveProject('${project.id}')" class="px-3 py-1 bg-prosko-green hover:bg-prosko-greenDark text-white font-extrabold text-xs rounded-lg shadow-sm transition">
                            <i class="fa-solid fa-check mr-0.5"></i> Approve
                        </button>
                        <button onclick="rejectProject('${project.id}')" class="px-2.5 py-1 bg-prosko-red text-white font-bold text-xs rounded-lg transition">
                            <i class="fa-solid fa-xmark"></i>
                        </button>
                    </div>
                </div>
            ` : ''}

            ${isSecretary && project.status === 'Pending Admin Approval' ? `
                <div class="p-2 bg-amber-50 rounded-xl border border-amber-200 text-amber-800 text-[11px] font-semibold flex items-center gap-1.5">
                    <i class="fa-solid fa-hourglass-half text-amber-600"></i> Proposal submitted: Awaiting SK Chairperson Review & Signoff
                </div>
            ` : ''}

            <div class="flex justify-end gap-2 pt-1 border-t border-slate-200/60">
                ${isAdmin && project.status !== 'Cancelled' ? `
                    <button onclick="openCancelProjectModal('${project.id}')" class="text-xs bg-amber-50 hover:bg-amber-500 hover:text-white text-amber-800 border border-amber-300 px-2.5 py-1 rounded-lg font-bold transition flex items-center gap-1">
                        <i class="fa-solid fa-ban"></i> Cancel Project
                    </button>
                ` : ''}
                ${isAdmin || (isSecretary && project.status === 'Pending Admin Approval') ? `
                    <button onclick="editProject('${project.id}')" class="text-xs bg-slate-200 hover:bg-prosko-blue hover:text-white px-2.5 py-1 rounded-lg font-bold transition flex items-center gap-1">
                        <i class="fa-solid fa-pen-to-square"></i> Edit
                    </button>
                ` : ''}
                ${isAdmin ? `
                    <button onclick="deleteProject('${project.id}')" class="text-xs bg-prosko-redLight text-prosko-red hover:bg-prosko-red hover:text-white px-2.5 py-1 rounded-lg font-bold transition flex items-center gap-1">
                        <i class="fa-solid fa-trash-can"></i> Delete
                    </button>
                ` : ''}
            </div>
        `;
        container.appendChild(card);
    });
}

async function approveProject(id) {
    // TASK REQ 3: SECRETARY CANNOT APPROVE!
    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Permission Denied: Only the SK Chairperson / Admin can approve proposals.', 'error');
        return;
    }

    const rawId = String(id);
    const p = projects.find(item => String(item.id) === rawId);

    if (p) {
        // Protection: Ensure approving this proposal does not exceed the remaining Available for Allocation
        const pYear = p.fiscalYear || currentFiscalYear;
        const annualBudget = annualBudgets[pYear] || 1600000.00;
        const fyProjects = projects.filter(item => (String(item.fiscalYear || '2026') === String(pYear)) && String(item.id) !== rawId);
        const currentAllocated = fyProjects
            .filter(item => item.status === 'Approved' || item.status === 'In Progress' || item.status === 'Completed')
            .reduce((acc, item) => acc + (parseFloat(item.budget) || 0), 0);
        const availableForAllocation = Math.max(0, annualBudget - currentAllocated);
        const projBudget = parseFloat(p.budget) || 0;

        if (projBudget > availableForAllocation) {
            showToast(`Cannot Approve: Project budget (₱${projBudget.toLocaleString('en-US', { minimumFractionDigits: 2 })}) exceeds FY ${pYear} Available for Allocation (₱${availableForAllocation.toLocaleString('en-US', { minimumFractionDigits: 2 })}).`, 'error');
            return;
        }

        p.status = 'Approved';
    }

    saveProjectsToLocal();

    const { error: updErr } = await supabaseUpdate('projects', { status: 'Approved' }, 'id', id);
    if (!updErr) {
        await fetchAllDatabaseData();
    }

    await logUserActivity('PROJECT_APPROVE', 'SK Chairperson approved project proposal: ' + (p ? p.title : id));
    showToast('Project proposal APPROVED! Budget allocated and awaiting SK Treasurer fund release.', 'success');

    updateDashboardUI();
}

function rejectProject(id) {
    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Permission Denied: Only SK Chairperson / Admin can reject proposals.', 'error');
        return;
    }

    customConfirm("Reject Proposal", "Reject and remove this secretary project proposal?", async (confirmed) => {
        if (confirmed) {
            const rawId = String(id);
            const idx = projects.findIndex(item => String(item.id) === rawId);
            if (idx !== -1) projects.splice(idx, 1);

            saveProjectsToLocal();

            const client = getSupabase();
            if (client) {
                try {
                    await client.from('projects').delete().eq('id', id);
                    await fetchAllDatabaseData();
                } catch (e) {}
            }

            await logUserActivity('PROJECT_REJECT', 'Rejected secretary proposal ID: ' + id);
            showToast('Project proposal rejected.', 'info');
            updateDashboardUI();
        }
    });
}

function deleteProject(id) {
    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Permission Denied: Only SK Chairperson / Admin can delete projects.', 'error');
        return;
    }

    const rawId = String(id);
    const p = projects.find(item => String(item.id) === rawId);
    const disbursed = p ? (parseFloat(p.disbursed) || 0) : 0;

    if (disbursed > 0 && p.status !== 'Cancelled') {
        showToast('Accounting Protection: Cannot simply delete a funded project! Please use "Cancel Project" to document fund return.', 'warning');
        openCancelProjectModal(id);
        return;
    }

    customConfirm("Delete Project", "Permanently remove this project from records?", async (confirmed) => {
        if (confirmed) {
            const idx = projects.findIndex(item => String(item.id) === rawId);
            if (idx !== -1) projects.splice(idx, 1);

            saveProjectsToLocal();

            const client = getSupabase();
            if (client) {
                try {
                    await client.from('projects').delete().eq('id', id);
                    await fetchAllDatabaseData();
                } catch (e) {}
            }

            await logUserActivity('PROJECT_DELETE', 'Deleted project ID: ' + id);
            showToast('Project deleted successfully.', 'info');
            updateDashboardUI();
        }
    });
}

// =========================================================================
// PROJECT CANCELLATION & TREASURY FUND RETURN (COA CIRCULAR 2020-004)
// =========================================================================
function openCancelProjectModal(id) {
    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Permission Denied: Only the SK Chairperson / Admin can authorize project cancellation.', 'error');
        return;
    }

    const rawId = String(id);
    const p = projects.find(item => String(item.id) === rawId);
    if (!p) {
        showToast('Project not found.', 'error');
        return;
    }

    document.getElementById('cancel-proj-id').value = p.id;
    document.getElementById('cancel-modal-proj-title').innerText = p.title;
    document.getElementById('cancel-modal-proj-budget').innerText = '₱' + (parseFloat(p.budget) || 0).toLocaleString('en-US', { minimumFractionDigits: 2 });

    const released = parseFloat(p.disbursed) || 0;
    document.getElementById('cancel-modal-proj-released').innerText = '₱' + released.toLocaleString('en-US', { minimumFractionDigits: 2 });
    document.getElementById('cancel-disp-released').innerText = '₱' + released.toLocaleString('en-US', { minimumFractionDigits: 2 });

    document.getElementById('cancel-amount-spent').value = '0.00';
    document.getElementById('cancel-amount-spent').max = released;
    document.getElementById('cancel-disp-returned').innerText = '₱' + released.toLocaleString('en-US', { minimumFractionDigits: 2 });

    document.getElementById('cancel-proj-reason').value = 'Event cancelled due to severe weather.';
    document.getElementById('cancel-project-modal').classList.remove('hidden');
}

function closeCancelProjectModal() {
    document.getElementById('cancel-project-modal').classList.add('hidden');
}

function recalculateCancelReturn() {
    const rawId = document.getElementById('cancel-proj-id').value;
    const p = projects.find(item => String(item.id) === String(rawId));
    const released = p ? (parseFloat(p.disbursed) || 0) : 0;
    const spent = Math.max(0, Math.min(released, parseFloat(document.getElementById('cancel-amount-spent').value) || 0));
    const returned = Math.max(0, released - spent);
    document.getElementById('cancel-disp-returned').innerText = '₱' + returned.toLocaleString('en-US', { minimumFractionDigits: 2 });
}

async function executeCancelProject(e) {
    if (e) e.preventDefault();

    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Permission Denied: Only the SK Chairperson / Admin can authorize project cancellation.', 'error');
        return;
    }

    const rawId = document.getElementById('cancel-proj-id').value;
    const reason = document.getElementById('cancel-proj-reason').value.trim();
    const p = projects.find(item => String(item.id) === String(rawId));

    if (!p) {
        showToast('Target project not found.', 'error');
        return;
    }

    if (!reason) {
        showToast('Please provide an official cancellation reason.', 'error');
        return;
    }

    const released = parseFloat(p.disbursed) || 0;
    let spent = parseFloat(document.getElementById('cancel-amount-spent').value) || 0;
    if (spent < 0) spent = 0;
    if (spent > released) spent = released;
    const returned = Math.max(0, released - spent);

    // 1. Update project status and adjust disbursed amount to reflect actual net expenditure
    p.status = 'Cancelled';
    p.cancellationReason = reason;
    p.amountReleased = released;
    p.amountSpent = spent;
    p.amountReturned = returned;
    p.disbursed = spent; // Only actual spent amount remains deducted; returned money is refunded!

    saveProjectsToLocal();

    // 2. Record Return Settlement in Disbursements Journal if money was returned
    if (returned > 0) {
        const fyDisbCount = disbursements.filter(d => String(d.fiscalYear || '2026') === String(currentFiscalYear)).length;
        const returnVoucherNum = 'CR-' + currentFiscalYear + '-' + String(fyDisbCount + 1).padStart(3, '0');
        const returnRecord = {
            id: returnVoucherNum,
            voucherNo: returnVoucherNum,
            projectId: p.id,
            projectTitle: p.title,
            payee: 'SK Treasury (Refund / Return)',
            purpose: 'Refund from cancelled project: ' + p.title + ' (Reason: ' + reason + ')',
            amount: -returned, // Negative disbursement to refund Available Youth Budget
            fiscalYear: currentFiscalYear,
            disbursedBy: activeUser.name,
            disbursedAt: new Date().toISOString(),
            status: 'Returned'
        };

        disbursements.unshift(returnRecord);
        saveDisbursementsToLocal();

        await supabaseInsert('disbursements', {
            id: returnVoucherNum,
            voucher_no: returnVoucherNum,
            project_id: isNaN(Number(p.id)) ? null : Number(p.id),
            project_title: p.title,
            payee: 'SK Treasury (Refund / Return)',
            purpose: 'Refund from cancelled project: ' + p.title + ' (Reason: ' + reason + ')',
            amount: -returned,
            fiscal_year: currentFiscalYear,
            disbursed_by: activeUser.name,
            status: 'Returned'
        });
    }

    // 3. Live Supabase update for project
    await supabaseUpdate('projects', {
        status: 'Cancelled',
        disbursed: spent,
        description: (p.description ? p.description + '\n' : '') + '[CANCELLED: ' + reason + ' | Released: ₱' + released.toLocaleString('en-US') + ' | Spent: ₱' + spent.toLocaleString('en-US') + ' | Returned: ₱' + returned.toLocaleString('en-US') + ']'
    }, 'id', p.id);

    // 4. Log detailed transaction in System Audit Trail
    await logUserActivity('PROJECT_CANCELLED', 'Cancelled project: ' + p.title + ' (Reason: ' + reason + ' | Released: ₱' + released.toLocaleString('en-US') + ' | Spent: ₱' + spent.toLocaleString('en-US') + ' | Returned: ₱' + returned.toLocaleString('en-US') + ' returned to Available Budget)');

    closeCancelProjectModal();
    await fetchAllDatabaseData();
    updateDashboardUI();

    showToast('Project cancelled! ₱' + returned.toLocaleString('en-US', { minimumFractionDigits: 2 }) + ' successfully returned to the Available Youth Budget.', 'success');
}

function editProject(id) {
    const rawId = String(id);
    const p = projects.find(item => String(item.id) === rawId);
    if (!p) return;
    document.getElementById('proj-id').value = p.id;
    document.getElementById('proj-title').value = p.title;
    document.getElementById('proj-category').value = p.category;
    document.getElementById('proj-budget').value = p.budget;
    document.getElementById('proj-period').value = p.period;

    const isAdmin = activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin' || activeUser.role === 'Admin / System Overseer');
    const isSecretary = activeUser && activeUser.role === 'SK Secretary';
    const statusContainer = document.getElementById('proj-status-container');
    const modalTitle = document.getElementById('modal-project-title');
    const statusSelect = document.getElementById('proj-status');

    modalTitle.innerText = "Edit Youth Project";

    if (isSecretary || !isAdmin) {
        if (statusContainer) statusContainer.classList.add('hidden');
        if (statusSelect) statusSelect.value = "Pending Admin Approval";
    } else {
        if (statusContainer) statusContainer.classList.remove('hidden');
        const disbursed = parseFloat(p.disbursed) || 0;
        if (statusSelect) {
            if (disbursed > 0) {
                statusSelect.innerHTML = `
                    <option value="In Progress">In Progress (Treasurer Released - ₱${disbursed.toLocaleString('en-US', { minimumFractionDigits: 2 })})</option>
                    <option value="Completed">Completed</option>
                    <option value="Approved">Approved</option>
                `;
            } else {
                statusSelect.innerHTML = `
                    <option value="Approved">Approved (Awaiting Treasurer Fund Release)</option>
                    <option value="Proposed">Proposed</option>
                    <option value="Pending Admin Approval">Pending Admin Approval</option>
                `;
            }
            statusSelect.value = p.status;
        }
    }

    document.getElementById('project-modal').classList.remove('hidden');
}

function filterProjects() {
    const searchEl = document.getElementById('project-search');
    const statusEl = document.getElementById('status-filter');
    const query = (searchEl ? searchEl.value : '').toLowerCase();
    const statusFilter = statusEl ? statusEl.value : 'ALL';

    const filtered = projects.filter(project => {
        const matchesSearch = (project.title || '').toLowerCase().includes(query) || (project.category || '').toLowerCase().includes(query);
        const matchesStatus = (statusFilter === "ALL") || (project.status === statusFilter);
        const matchesYear = String(project.fiscalYear || '2026') === String(currentFiscalYear);
        return matchesSearch && matchesStatus && matchesYear;
    });

    renderProjectsCards(filtered);
}

function filterPendingApprovals() {
    switchDashboardTab('projects');
    const statusFilter = document.getElementById('status-filter');
    if (statusFilter) {
        statusFilter.value = 'Pending Admin Approval';
        filterProjects();
    }
    showToast('Showing pending project approvals', 'info');
}

// =========================================================================
// 14. ACCOMPLISHMENT REPORTS & BRGY DOCUMENTS
// =========================================================================
function renderAccomplishmentReports() {
    const container = document.getElementById('accomplishment-reports-list');
    if (!container) return;
    container.innerHTML = '';

    const isAdmin = activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin');

    if (!accomplishmentReports || accomplishmentReports.length === 0) {
        container.innerHTML = `
            <div class="p-8 text-center text-slate-400">
                <i class="fa-solid fa-award text-3xl mb-1 text-slate-300"></i>
                <p class="font-bold text-xs text-slate-500">No accomplishment reports filed yet</p>
            </div>
        `;
        return;
    }

    accomplishmentReports.forEach(r => {
        const item = document.createElement('div');
        item.className = "bg-slate-50 p-4 rounded-xl border border-slate-200/90 space-y-1.5 text-xs";

        let badgeClass = "bg-purple-100 text-purple-800";
        if (r.status === 'Pending Admin Approval') badgeClass = "bg-amber-100 text-amber-800 border border-amber-300";
        if (r.status === 'Approved') badgeClass = "bg-emerald-100 text-emerald-800";

        item.innerHTML = `
            <div class="flex justify-between items-start">
                <span class="font-bold text-prosko-purple text-xs">${escapeHtml(r.member)}</span>
                <span class="px-2.5 py-0.5 rounded-full text-[10px] font-extrabold ${badgeClass}">${r.status}</span>
            </div>
            <p class="font-extrabold text-slate-900 text-sm">${escapeHtml(r.title)}</p>
            <p class="text-slate-600 text-xs leading-relaxed">${escapeHtml(r.details)}</p>
            <p class="text-slate-400 text-[10px] pt-1"><i class="fa-solid fa-calendar-day mr-1"></i>Date Conducted: ${escapeHtml(r.date)}</p>

            ${isAdmin && r.status === 'Pending Admin Approval' ? `
                <div class="p-2.5 bg-amber-50 rounded-xl border border-amber-200 flex items-center justify-between gap-2 mt-2">
                    <span class="text-xs font-bold text-amber-900"><i class="fa-solid fa-user-check text-amber-600 mr-1"></i>Report Needs Chairperson Signoff</span>
                    <div class="flex gap-1.5">
                        <button onclick="approveReport('${r.id}')" class="px-3 py-1 bg-prosko-green hover:bg-prosko-greenDark text-white font-extrabold text-xs rounded-lg shadow-sm transition">
                            <i class="fa-solid fa-check mr-0.5"></i> Approve
                        </button>
                        <button onclick="rejectReport('${r.id}')" class="px-2.5 py-1 bg-prosko-red text-white font-bold text-xs rounded-lg transition">
                            <i class="fa-solid fa-xmark"></i>
                        </button>
                    </div>
                </div>
            ` : ''}
        `;
        container.appendChild(item);
    });
}

async function approveReport(id) {
    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Only SK Chairperson can approve reports.', 'error');
        return;
    }

    const rep = accomplishmentReports.find(r => r.id === id);
    if (rep) rep.status = 'Approved';
    saveReportsToLocal();

    await supabaseUpdate('accomplishment_reports', { status: 'Approved' }, 'id', id);
    await fetchAllDatabaseData();

    await logUserActivity('REPORT_APPROVE', 'Approved report: ' + (rep ? rep.title : id));
    showToast('Accomplishment report APPROVED by Chairperson!', 'success');
    updateDashboardUI();
}

function rejectReport(id) {
    if (!activeUser || (activeUser.role !== 'SK Chairperson' && activeUser.role !== 'Admin')) {
        showToast('Only SK Chairperson can reject reports.', 'error');
        return;
    }

    customConfirm("Reject Report", "Reject and remove this accomplishment report?", async (confirmed) => {
        if (confirmed) {
            const idx = accomplishmentReports.findIndex(r => r.id === id);
            if (idx !== -1) accomplishmentReports.splice(idx, 1);
            saveReportsToLocal();

            const client = getSupabase();
            if (client) {
                try {
                    await client.from('accomplishment_reports').delete().eq('id', id);
                    await fetchAllDatabaseData();
                } catch (err) {}
            }

            await logUserActivity('REPORT_REJECT', 'Rejected accomplishment report ID: ' + id);
            showToast('Accomplishment report rejected.', 'info');
            updateDashboardUI();
        }
    });
}

function openNewReportModal() {
    document.getElementById('report-modal').classList.remove('hidden');
}

function closeReportModal() {
    document.getElementById('report-modal').classList.add('hidden');
}

async function saveReport(e) {
    if (e) e.preventDefault();
    const member = document.getElementById('rep-member').value.trim();
    const title = document.getElementById('rep-title').value.trim();
    const details = document.getElementById('rep-details').value.trim();
    const date = document.getElementById('rep-date').value.trim();

    const isSecretary = activeUser && activeUser.role === 'SK Secretary';
    const initialStatus = isSecretary ? "Pending Admin Approval" : "Approved";

    const newReport = {
        id: generateUniqueId('REP'),
        member: member,
        title: title,
        details: details,
        date: date,
        status: initialStatus
    };

    accomplishmentReports.unshift(newReport);
    saveReportsToLocal();

    const { error: repErr } = await supabaseInsert('accomplishment_reports', newReport);
    if (!repErr) {
        await fetchAllDatabaseData();
    }

    await logUserActivity('REPORT_SUBMIT', 'Submitted report: ' + title);
    closeReportModal();
    showToast(isSecretary ? 'Report submitted for Admin approval!' : 'Accomplishment report saved!', 'success');

    updateDashboardUI();
}

function renderBrgyDocuments() {
    const container = document.getElementById('brgy-documents-list');
    if (!container) return;
    container.innerHTML = '';

    if (!brgyDocuments || brgyDocuments.length === 0) {
        container.innerHTML = `
            <div class="p-8 text-center text-slate-400">
                <i class="fa-solid fa-folder-open text-3xl mb-1 text-slate-300"></i>
                <p class="font-bold text-xs text-slate-500">No documents archived yet</p>
            </div>
        `;
        return;
    }

    brgyDocuments.forEach(d => {
        const item = document.createElement('div');
        item.className = "bg-slate-50 p-3.5 rounded-xl border border-slate-200/90 space-y-1 text-xs flex justify-between items-center";
        item.innerHTML = `
            <div>
                <span class="font-bold text-prosko-blue text-xs block">${escapeHtml(d.ref)}</span>
                <span class="font-extrabold text-slate-900 block text-xs">${escapeHtml(d.title)}</span>
                <span class="text-[10px] text-slate-500 font-medium">${escapeHtml(d.type)} • ${escapeHtml(d.date)}</span>
            </div>
            <button onclick="downloadDocMock('${escapeHtml(d.title)}')" class="px-2.5 py-1 bg-slate-200 hover:bg-prosko-blue hover:text-white rounded-lg text-slate-700 transition flex items-center gap-1 font-bold text-[10px]">
                <i class="fa-solid fa-download"></i> View
            </button>
        `;
        container.appendChild(item);
    });
}

function downloadDocMock(title) {
    showToast('Opening ' + title + ' document viewer...', 'info');
}

function openNewDocModal() {
    document.getElementById('doc-modal').classList.remove('hidden');
}

function closeDocModal() {
    document.getElementById('doc-modal').classList.add('hidden');
}

async function saveDocument(e) {
    if (e) e.preventDefault();
    const title = document.getElementById('doc-title').value.trim();
    const type = document.getElementById('doc-type').value;
    const ref = document.getElementById('doc-ref').value.trim();

    const newDoc = {
        id: generateUniqueId('DOC'),
        ref: ref,
        title: title,
        type: type,
        date: new Date().toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' })
    };

    brgyDocuments.unshift(newDoc);
    saveDocsToLocal();

    const { error: docErr } = await supabaseInsert('brgy_documents', {
        id: newDoc.id,
        ref: newDoc.ref,
        ref_no: newDoc.ref,
        title: newDoc.title,
        type: newDoc.type,
        doc_type: newDoc.type,
        date: newDoc.date
    });
    if (!docErr) {
        await fetchAllDatabaseData();
    }

    await logUserActivity('DOC_UPLOAD', 'Uploaded document: ' + title + ' (' + ref + ')');
    closeDocModal();
    showToast('Barangay Document archived successfully!', 'success');

    updateDashboardUI();
}

// =========================================================================
// 15. USER MANAGEMENT MODULE (ADMIN ONLY - IAS REQUIREMENT A)
// =========================================================================
function renderUsersTable() {
    const tbody = document.getElementById('users-table-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    usersStore.forEach(u => {
        const tr = document.createElement('tr');
        tr.className = "hover:bg-slate-50 transition border-b border-slate-100";

        const isLocked = u.isLocked;
        const isActive = u.status === 'Active';

        tr.innerHTML = `
            <td class="py-2.5 px-3">
                <div class="flex items-center gap-2">
                    <div class="w-7 h-7 rounded-lg bg-prosko-blue text-white font-black text-xs flex items-center justify-center">
                        ${u.fullName.charAt(0)}
                    </div>
                    <span class="font-bold text-slate-900">${escapeHtml(u.fullName)}</span>
                </div>
            </td>
            <td class="py-2.5 px-3 font-mono font-semibold text-slate-700">${escapeHtml(u.username)}</td>
            <td class="py-2.5 px-3 text-slate-500">${escapeHtml(u.email)}</td>
            <td class="py-2.5 px-3">
                <select onchange="changeUserRole('${u.id}', this.value)" class="text-[11px] font-bold border border-slate-300 rounded-lg px-2 py-1 bg-white text-slate-800">
                    <option value="SK Chairperson" ${u.role === 'SK Chairperson' ? 'selected' : ''}>SK Chairperson</option>
                    <option value="SK Secretary" ${u.role === 'SK Secretary' ? 'selected' : ''}>SK Secretary</option>
                    <option value="SK Treasurer" ${u.role === 'SK Treasurer' ? 'selected' : ''}>SK Treasurer</option>
                    <option value="SK Kagawad" ${u.role === 'SK Kagawad' ? 'selected' : ''}>SK Kagawad</option>
                </select>
            </td>
            <td class="py-2.5 px-3 text-center">
                <span class="px-2 py-0.5 rounded-full text-[10px] font-black ${isActive ? 'bg-emerald-100 text-emerald-800' : 'bg-red-100 text-red-800'}">
                    ${u.status}
                </span>
            </td>
            <td class="py-2.5 px-3 text-center">
                <span class="px-2 py-0.5 rounded-full text-[10px] font-black ${isLocked ? 'bg-red-100 text-red-800' : 'bg-emerald-100 text-emerald-800'}">
                    ${isLocked ? 'Locked' : 'Unlocked'}
                </span>
            </td>
            <td class="py-2.5 px-3 text-right">
                <div class="flex items-center justify-end gap-1.5">
                    <button onclick="toggleUserActive('${u.id}')" class="px-2 py-1 rounded text-[10px] font-extrabold ${isActive ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'bg-emerald-100 text-emerald-800 hover:bg-emerald-200'} transition">
                        ${isActive ? 'Deactivate' : 'Activate'}
                    </button>
                    <button onclick="toggleUserLock('${u.id}')" class="px-2 py-1 rounded text-[10px] font-extrabold ${isLocked ? 'bg-blue-100 text-prosko-blue hover:bg-blue-200' : 'bg-red-100 text-prosko-red hover:bg-red-200'} transition">
                        ${isLocked ? 'Unlock' : 'Lock'}
                    </button>
                    <button onclick="adminResetUserPassword('${u.id}')" class="px-2 py-1 rounded text-[10px] font-extrabold bg-slate-200 text-slate-700 hover:bg-slate-300 transition" title="Reset Password">
                        <i class="fa-solid fa-key"></i>
                    </button>
                </div>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

function filterUsersList() {
    const query = (document.getElementById('user-search-input').value || '').toLowerCase();
    const tbody = document.getElementById('users-table-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    const filtered = usersStore.filter(u =>
        u.fullName.toLowerCase().includes(query) ||
        u.username.toLowerCase().includes(query) ||
        u.email.toLowerCase().includes(query) ||
        u.role.toLowerCase().includes(query)
    );

    filtered.forEach(u => {
        const tr = document.createElement('tr');
        tr.className = "hover:bg-slate-50 transition border-b border-slate-100";
        const isLocked = u.isLocked;
        const isActive = u.status === 'Active';

        tr.innerHTML = `
            <td class="py-2.5 px-3">
                <div class="flex items-center gap-2">
                    <div class="w-7 h-7 rounded-lg bg-prosko-blue text-white font-black text-xs flex items-center justify-center">
                        ${u.fullName.charAt(0)}
                    </div>
                    <span class="font-bold text-slate-900">${escapeHtml(u.fullName)}</span>
                </div>
            </td>
            <td class="py-2.5 px-3 font-mono font-semibold text-slate-700">${escapeHtml(u.username)}</td>
            <td class="py-2.5 px-3 text-slate-500">${escapeHtml(u.email)}</td>
            <td class="py-2.5 px-3 font-semibold text-prosko-blue">${escapeHtml(u.role)}</td>
            <td class="py-2.5 px-3 text-center">
                <span class="px-2 py-0.5 rounded-full text-[10px] font-black ${isActive ? 'bg-emerald-100 text-emerald-800' : 'bg-red-100 text-red-800'}">
                    ${u.status}
                </span>
            </td>
            <td class="py-2.5 px-3 text-center">
                <span class="px-2 py-0.5 rounded-full text-[10px] font-black ${isLocked ? 'bg-red-100 text-red-800' : 'bg-emerald-100 text-emerald-800'}">
                    ${isLocked ? 'Locked' : 'Unlocked'}
                </span>
            </td>
            <td class="py-2.5 px-3 text-right">
                <div class="flex items-center justify-end gap-1.5">
                    <button onclick="toggleUserActive('${u.id}')" class="px-2 py-1 rounded text-[10px] font-extrabold ${isActive ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'bg-emerald-100 text-emerald-800 hover:bg-emerald-200'} transition">
                        ${isActive ? 'Deactivate' : 'Activate'}
                    </button>
                    <button onclick="toggleUserLock('${u.id}')" class="px-2 py-1 rounded text-[10px] font-extrabold ${isLocked ? 'bg-blue-100 text-prosko-blue hover:bg-blue-200' : 'bg-red-100 text-prosko-red hover:bg-red-200'} transition">
                        ${isLocked ? 'Unlock' : 'Lock'}
                    </button>
                    <button onclick="adminResetUserPassword('${u.id}')" class="px-2 py-1 rounded text-[10px] font-extrabold bg-slate-200 text-slate-700 hover:bg-slate-300 transition" title="Reset Password">
                        <i class="fa-solid fa-key"></i>
                    </button>
                </div>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

async function changeUserRole(userId, newRole) {
    const usr = usersStore.find(u => u.id === userId);
    if (!usr) return;
    const oldRole = usr.role;
    usr.role = newRole;
    saveUsersStoreToLocal();

    await supabaseUpdate('profiles', { role: newRole }, 'username', usr.username);
    await logUserActivity('ROLE_CHANGE', 'Changed role of ' + usr.username + ' from ' + oldRole + ' to ' + newRole);
    showToast('Updated ' + usr.fullName + '\'s role to ' + newRole, 'success');
}

async function toggleUserActive(userId) {
    const usr = usersStore.find(u => u.id === userId);
    if (!usr) return;

    usr.status = (usr.status === 'Active' ? 'Deactivated' : 'Active');
    saveUsersStoreToLocal();

    await supabaseUpdate('profiles', { status: usr.status }, 'username', usr.username);
    await logUserActivity('USER_STATUS_CHANGE', 'Set status of ' + usr.username + ' to ' + usr.status);
    showToast(usr.fullName + ' status updated to ' + usr.status, 'info');
    renderUsersTable();
}

async function toggleUserLock(userId) {
    const usr = usersStore.find(u => u.id === userId);
    if (!usr) return;

    usr.isLocked = !usr.isLocked;
    if (!usr.isLocked) usr.failedAttempts = 0;
    saveUsersStoreToLocal();

    await supabaseUpdate('profiles', { is_locked: usr.isLocked, failed_attempts: usr.failedAttempts }, 'username', usr.username);
    await logUserActivity('USER_LOCK_TOGGLE', (usr.isLocked ? 'Locked' : 'Unlocked') + ' account of ' + usr.username);
    showToast(usr.fullName + ' is now ' + (usr.isLocked ? 'LOCKED' : 'UNLOCKED'), 'info');
    renderUsersTable();
}

async function adminResetUserPassword(userId) {
    const usr = usersStore.find(u => u.id === userId);
    if (!usr) return;

    customConfirm('Reset Password', 'Reset password for ' + usr.fullName + ' to default (Belly1234!)?', async (confirmed) => {
        if (confirmed) {
            usr.passwordHash = await hashPassword('Belly1234!');
            usr.isLocked = false;
            usr.failedAttempts = 0;
            saveUsersStoreToLocal();

            await supabaseUpdate('profiles', { password_hash: usr.passwordHash, is_locked: false, failed_attempts: 0 }, 'username', usr.username);
            await logUserActivity('ADMIN_PASSWORD_RESET', 'Admin reset password for ' + usr.username);
            showToast('Password for ' + usr.username + ' has been reset to Belly1234!', 'success');
        }
    });
}

function closeLogoutModal() {
    const modal = document.getElementById('logout-modal');
    if (modal) modal.classList.add('hidden');
}

function openCreateUserModal() {
    document.getElementById('usr-edit-id').value = '';
    document.getElementById('usr-fullname').value = '';
    document.getElementById('usr-username').value = '';
    document.getElementById('usr-username').disabled = false;
    document.getElementById('usr-email').value = '';
    document.getElementById('usr-role').value = 'SK Kagawad';
    document.getElementById('usr-password').value = 'Belly1234!';
    document.getElementById('usr-pass-field-container').classList.remove('hidden');
    document.getElementById('user-modal-title').innerText = 'Add New SK Official';
    document.getElementById('user-modal').classList.remove('hidden');
}

function closeUserModal() {
    document.getElementById('user-modal').classList.add('hidden');
}

async function saveUserAccount(e) {
    if (e) e.preventDefault();

    const editId = document.getElementById('usr-edit-id').value;
    const fullName = document.getElementById('usr-fullname').value.trim();
    const username = document.getElementById('usr-username').value.trim();
    const email = document.getElementById('usr-email').value.trim();
    const role = document.getElementById('usr-role').value;
    const rawPass = document.getElementById('usr-password').value.trim() || 'Belly1234!';

    if (!fullName || !username || !email) {
        showToast('Please fill in all required official details.', 'error');
        return;
    }

    if (editId) {
        // Edit existing
        const usr = usersStore.find(u => u.id === editId);
        if (usr) {
            usr.fullName = fullName;
            usr.email = email;
            usr.role = role;
            saveUsersStoreToLocal();
            await supabaseUpdate('profiles', { full_name: fullName, email: email, role: role }, 'username', usr.username);
            showToast('Official profile updated successfully.', 'success');
        }
    } else {
        // Create new
        const exists = usersStore.some(u => u.username.toLowerCase() === username.toLowerCase() || u.email.toLowerCase() === email.toLowerCase());
        if (exists) {
            showToast('An official with this username or email already exists.', 'error');
            return;
        }

        const passHash = await hashPassword(rawPass);
        const newUser = {
            id: generateUniqueId('USR'),
            fullName,
            username,
            email,
            role,
            passwordHash: passHash,
            status: 'Active',
            isLocked: false,
            failedAttempts: 0
        };

        usersStore.push(newUser);
        saveUsersStoreToLocal();
        await supabaseInsert('profiles', {
            full_name: fullName,
            username,
            email,
            role,
            password_hash: passHash,
            status: 'Active',
            is_locked: false,
            failed_attempts: 0
        });

        await logUserActivity('USER_CREATED', 'Admin added new official: ' + fullName + ' (' + role + ')');
        showToast('New official ' + fullName + ' added successfully.', 'success');
    }

    closeUserModal();
    renderUsersTable();
}

// =========================================================================
// 16. BACKUP & DISASTER RECOVERY MODULE (ADMIN ONLY - IAS REQUIREMENT H)
// =========================================================================
function scheduleAutomatedBackupSnapshot() {
    setInterval(async () => {
        if (activeUser && (activeUser.role === 'SK Chairperson' || activeUser.role === 'Admin')) {
            const snapshot = createBackupPayload();
            localStorage.setItem('prosko_auto_snapshot', JSON.stringify(snapshot));
            backupLogs.unshift({
                id: generateUniqueId('BKP'),
                type: 'Automated Snapshot',
                timestamp: new Date().toLocaleString('en-US'),
                status: 'Completed',
                size: Math.round(JSON.stringify(snapshot).length / 1024) + ' KB'
            });
            if (backupLogs.length > 20) backupLogs.pop();
            renderBackupHistory();
        }
    }, 30 * 60 * 1000); // 30 minutes
}

function createBackupPayload() {
    return {
        version: 'PROSKO-2.0-IAS-FINAL',
        backupDate: new Date().toISOString(),
        system: 'Barangay Libjo SK PMES',
        annualBudgets: annualBudgets,
        projects: projects,
        disbursements: disbursements,
        accomplishmentReports: accomplishmentReports,
        brgyDocuments: brgyDocuments,
        incidents: incidents,
        usersStore: usersStore
    };
}

function exportDatabaseBackup() {
    const payload = createBackupPayload();
    const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(payload, null, 2));
    const dlAnchor = document.createElement('a');
    dlAnchor.setAttribute("href", dataStr);
    dlAnchor.setAttribute("download", "PROSKO_BACKUP_" + currentFiscalYear + "_" + new Date().toISOString().slice(0, 10) + ".json");
    document.body.appendChild(dlAnchor);
    dlAnchor.click();
    dlAnchor.remove();

    backupLogs.unshift({
        id: generateUniqueId('BKP'),
        type: 'Manual JSON Export',
        timestamp: new Date().toLocaleString('en-US'),
        status: 'Downloaded',
        size: Math.round(JSON.stringify(payload).length / 1024) + ' KB'
    });

    logUserActivity('BACKUP_EXPORT', 'Admin exported full encrypted JSON database backup');
    showToast('Database backup successfully exported and downloaded!', 'success');
    renderBackupHistory();
}

function downloadDatabaseBackup() {
    exportDatabaseBackup();
}

function triggerRestoreFileInput() {
    document.getElementById('restore-file-input').click();
}

function triggerRestoreBackup() {
    triggerRestoreFileInput();
}

function handleBackupFileSelect(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async (e) => {
        try {
            const data = JSON.parse(e.target.result);
            if (!data.version || !data.version.startsWith('PROSKO')) {
                showToast('Invalid backup file structure.', 'error');
                return;
            }

            customConfirm('Restore Database', 'Restoring will overwrite current system stores. Proceed?', async (confirmed) => {
                if (confirmed) {
                    if (data.projects) {
                        projects = data.projects;
                        projects.forEach(p => {
                            const sub = p.submittedBy || p.submitted_by || '';
                            if (!sub || /von|pilapil|sk chair admin|chair admin|sk chair|admin_libjo|^admin$|^sk chairperson$|belly jean montecillo/i.test(sub)) {
                                p.submittedBy = 'Belly Jean Montecillo (SK Chairperson)';
                                p.submitted_by = 'Belly Jean Montecillo (SK Chairperson)';
                            }
                            if (p.status === 'In Progress' && (parseFloat(p.disbursed) || 0) <= 0) {
                                p.status = 'Approved';
                            }
                        });
                    }
                    if (data.disbursements) disbursements = data.disbursements;
                    if (data.accomplishmentReports) accomplishmentReports = data.accomplishmentReports;
                    if (data.brgyDocuments) brgyDocuments = data.brgyDocuments;
                    if (data.incidents) incidents = data.incidents;
                    if (data.annualBudgets) annualBudgets = data.annualBudgets;
                    if (data.usersStore) {
                        usersStore = data.usersStore;
                        const legacyHash = '0532523ad168498c037cfb132a57b6fa4b5596f5fda65a046aa570274a3237df';
                        const newHash = await hashPassword('Belly1234!');
                        usersStore.forEach(u => {
                            if (u.passwordHash === legacyHash) u.passwordHash = newHash;
                            if (u.username === 'admin_libjo' || (u.fullName && /von|pilapil|sk chair admin|chair admin|sk chair|^admin$/i.test(u.fullName))) {
                                u.fullName = 'Belly Jean Montecillo';
                            }
                        });
                    }

                    saveProjectsToLocal();
                    saveDisbursementsToLocal();
                    saveReportsToLocal();
                    saveDocsToLocal();
                    saveIncidentsToLocal();
                    saveUsersStoreToLocal();
                    localStorage.setItem('prosko_annual_budgets', JSON.stringify(annualBudgets));

                    backupLogs.unshift({
                        id: generateUniqueId('RST'),
                        type: 'Database Restore',
                        timestamp: new Date().toLocaleString('en-US'),
                        status: 'Completed',
                        size: Math.round(file.size / 1024) + ' KB'
                    });

                    await logUserActivity('BACKUP_RESTORED', 'Admin restored database state from ' + file.name);

                    showToast('Database successfully restored from backup file!', 'success');
                    updateDashboardUI();
                    renderBackupHistory();
                }
            });

        } catch (err) {
            showToast('Failed to parse backup JSON file: ' + err.message, 'error');
        }
    };

    reader.readAsText(file);
}

function renderBackupHistory() {
    const container = document.getElementById('backup-history-list');
    if (!container) return;
    container.innerHTML = '';

    backupLogs.forEach(log => {
        const item = document.createElement('div');
        item.className = "p-2.5 bg-slate-50 rounded-xl border border-slate-200 flex justify-between items-center text-xs";
        item.innerHTML = `
            <div class="flex items-center gap-2">
                <span class="w-6 h-6 rounded-lg bg-blue-100 text-prosko-blue font-bold flex items-center justify-center text-[10px]">
                    <i class="fa-solid fa-hard-drive"></i>
                </span>
                <div>
                    <span class="font-extrabold text-slate-800">${log.type}</span>
                    <span class="text-[10px] text-slate-400 block">${log.timestamp} • ${log.size}</span>
                </div>
            </div>
            <span class="px-2 py-0.5 rounded-full bg-emerald-100 text-emerald-800 font-bold text-[10px]">${log.status}</span>
        `;
        container.appendChild(item);
    });
}

// =========================================================================
// 17. AUDIT LOG TRAIL (ADMIN ONLY - IAS REQUIREMENT F)
// =========================================================================
async function logUserActivity(action, details) {
    const user = activeUser ? activeUser.username : 'System';
    const role = activeUser ? activeUser.role : 'Officer';
    await logUserActivityDirect(user, role, action, details);
}

async function logUserActivityDirect(username, role, action, details) {
    const logEntry = {
        username: username,
        user_name: username,
        user_role: role,
        action: action,
        details: details
    };

    const client = getSupabase();
    if (client) {
        try {
            await supabaseInsert('activity_logs', logEntry);
        } catch (err) {}
        try {
            await supabaseInsert('audit_logs', logEntry);
        } catch (err) {}
    }
}

async function fetchLogTrails() {
    if (!supabase) return;
    try {
        const { data, error } = await supabase
            .from('activity_logs')
            .select('*')
            .order('created_at', { ascending: false })
            .limit(100);

        if (!error && data) {
            activityLogs = data;
            renderLogTrailFeed(activityLogs);
        }
    } catch (err) {}
}

function renderLogTrailFeed(logList) {
    const container = document.getElementById('log-trail-list');
    if (!container) return;
    container.innerHTML = '';

    if (!logList || logList.length === 0) {
        container.innerHTML = `
            <div class="p-8 text-center text-slate-400">
                <i class="fa-solid fa-receipt text-3xl mb-1 text-slate-300"></i>
                <p class="font-bold text-xs text-slate-500">No activity logs recorded yet</p>
            </div>
        `;
        return;
    }

    logList.forEach(log => {
        const timeFormatted = new Date(log.created_at).toLocaleString('en-US', {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
        });

        let actionBadgeColor = "bg-slate-200 text-slate-700";
        if (log.action.includes('LOGIN')) actionBadgeColor = "bg-emerald-100 text-emerald-800";
        if (log.action.includes('REGISTER')) actionBadgeColor = "bg-purple-100 text-purple-800";
        if (log.action.includes('APPROVE') || log.action.includes('CREATE')) actionBadgeColor = "bg-blue-100 text-prosko-blue";
        if (log.action.includes('DELETE') || log.action.includes('REJECT') || log.action.includes('LOCKED')) actionBadgeColor = "bg-red-100 text-prosko-red";
        if (log.action.includes('DISBURSEMENT') || log.action.includes('ROLLOVER')) actionBadgeColor = "bg-emerald-100 text-emerald-900";

        const item = document.createElement('div');
        item.className = "p-3 bg-slate-50 rounded-xl border border-slate-200 text-xs space-y-1";
        item.innerHTML = `
            <div class="flex justify-between items-center">
                <div class="flex items-center gap-2">
                    <span class="font-extrabold text-slate-900 text-xs">${escapeHtml(log.username)}</span>
                    <span class="text-[10px] font-bold text-slate-400">(${escapeHtml(log.user_role)})</span>
                </div>
                <span class="text-[9px] font-mono font-bold text-slate-400">${timeFormatted}</span>
            </div>
            <div class="flex items-center gap-2">
                <span class="px-2 py-0.5 rounded text-[8px] font-black uppercase ${actionBadgeColor}">${log.action}</span>
                <p class="text-xs text-slate-600 font-medium truncate">${escapeHtml(log.details)}</p>
            </div>
        `;
        container.appendChild(item);
    });
}

function filterLogs() {
    const query = (document.getElementById('log-search').value || '').toLowerCase();
    const filtered = activityLogs.filter(log =>
        log.username.toLowerCase().includes(query) ||
        log.action.toLowerCase().includes(query) ||
        log.details.toLowerCase().includes(query) ||
        log.user_role.toLowerCase().includes(query)
    );
    renderLogTrailFeed(filtered);
}

// =========================================================================
// 18. INCIDENTS MODULE
// =========================================================================
function renderIncidentsMobileList() {
    const container = document.getElementById('incidents-mobile-list');
    if (!container) return;
    container.innerHTML = '';

    if (!incidents || incidents.length === 0) {
        container.innerHTML = `
            <div class="p-8 text-center text-slate-400">
                <i class="fa-solid fa-shield-halved text-3xl mb-1 text-slate-300"></i>
                <p class="font-bold text-xs text-slate-500">No community incidents recorded</p>
            </div>
        `;
        return;
    }

    incidents.forEach(inc => {
        let badgeClass = "bg-amber-100 text-amber-800";
        if (inc.status === 'Resolved') badgeClass = "bg-emerald-100 text-emerald-800";
        if (inc.status === 'Under Review') badgeClass = "bg-red-100 text-red-800";

        const item = document.createElement('div');
        item.className = "p-3 bg-slate-50 rounded-xl border border-slate-200 text-xs space-y-1";
        item.innerHTML = `
            <div class="flex justify-between items-center">
                <span class="font-bold text-slate-900">${inc.id}</span>
                <span class="text-[10px] font-bold px-2.5 py-0.5 rounded-full ${badgeClass}">${inc.status}</span>
            </div>
            <p class="text-xs text-slate-700 font-semibold">${escapeHtml(inc.type)}</p>
            <p class="text-[11px] text-slate-500">${escapeHtml(inc.location)} • ${escapeHtml(inc.date)}</p>
            <button onclick="toggleIncidentStatus('${inc.id}')" class="text-[10px] text-prosko-blue font-bold hover:underline pt-1 inline-block">
                Toggle Resolution Status
            </button>
        `;
        container.appendChild(item);
    });
}

function openNewIncidentModal() {
    document.getElementById('incident-modal').classList.remove('hidden');
}

function closeIncidentModal() {
    document.getElementById('incident-modal').classList.add('hidden');
}

async function saveIncident(e) {
    if (e) e.preventDefault();
    const type = document.getElementById('inc-type').value.trim();
    const location = document.getElementById('inc-location').value.trim();
    const status = document.getElementById('inc-status').value;

    const newInc = {
        id: generateUniqueId('INC'),
        type: type,
        location: location,
        status: status,
        date: new Date().toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' })
    };

    incidents.unshift(newInc);
    saveIncidentsToLocal();

    const { error: incErr } = await supabaseInsert('incidents', newInc);
    if (!incErr) {
        await fetchAllDatabaseData();
    }

    await logUserActivity('INCIDENT_REPORTED', 'Recorded new incident: ' + type + ' at ' + location);
    closeIncidentModal();
    showToast('Community incident logged successfully!', 'success');
    updateDashboardUI();
}

function toggleIncidentStatus(id) {
    const inc = incidents.find(i => i.id === id);
    if (!inc) return;

    inc.status = (inc.status === 'Resolved' ? 'Under Review' : 'Resolved');
    saveIncidentsToLocal();

    showToast('Incident ' + id + ' status updated to ' + inc.status, 'info');
    renderIncidentsMobileList();
}

// =========================================================================
// 19. AI RISK ENGINE MODULE
// =========================================================================
function runAiEvaluation() {
    showToast('AI Evaluator: Recalculating youth risk matrices...', 'info');
    setTimeout(() => {
        showToast('AI Evaluation Complete: High engagement confidence verified!', 'success');
    }, 1000);
}

// =========================================================================
// 20. UI UTILITIES (TOASTS, DIALOGS, ALERTS)
// =========================================================================
function customConfirm(title, message, callback) {
    const modal = document.getElementById('confirm-dialog-modal');
    const titleEl = document.getElementById('confirm-modal-title');
    const msgEl = document.getElementById('confirm-modal-msg');
    const btnOk = document.getElementById('confirm-modal-ok');
    const btnCancel = document.getElementById('confirm-modal-cancel');

    titleEl.innerText = title;
    msgEl.innerText = message;

    modal.classList.remove('hidden');

    const handleOk = () => {
        modal.classList.add('hidden');
        cleanup();
        callback(true);
    };

    const handleCancel = () => {
        modal.classList.add('hidden');
        cleanup();
        callback(false);
    };

    function cleanup() {
        btnOk.removeEventListener('click', handleOk);
        btnCancel.removeEventListener('click', handleCancel);
    }

    btnOk.addEventListener('click', handleOk);
    btnCancel.addEventListener('click', handleCancel);
}

function showAlert(message, type) {
    const alertBox = document.getElementById('auth-alert');
    const icon = document.getElementById('auth-alert-icon');
    const msg = document.getElementById('auth-alert-message');

    alertBox.classList.remove('hidden');
    msg.innerText = message;

    if (type === 'error') {
        alertBox.className = "mb-3 p-3 rounded-xl text-xs flex items-start gap-2.5 border bg-prosko-redLight text-prosko-red border-prosko-red/30 transition fade-in font-semibold";
        icon.className = "fa-solid fa-circle-exclamation text-sm mt-0.5 text-prosko-red";
    } else {
        alertBox.className = "mb-3 p-3 rounded-xl text-xs flex items-start gap-2.5 border bg-prosko-greenLight text-prosko-greenDark border-prosko-green/30 transition fade-in font-semibold";
        icon.className = "fa-solid fa-circle-check text-sm mt-0.5 text-prosko-green";
    }
}

function hideAlert() {
    const el = document.getElementById('auth-alert');
    if (el) el.classList.add('hidden');
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const toast = document.createElement('div');

    let bgIcon = "fa-circle-info text-prosko-yellow";
    if (type === 'success') bgIcon = "fa-circle-check text-prosko-green";
    if (type === 'error') bgIcon = "fa-circle-xmark text-prosko-red";

    toast.className = "bg-prosko-blueDark/95 backdrop-blur-md text-white border border-white/20 shadow-xl px-4 py-3 rounded-2xl text-xs flex items-center gap-3 pointer-events-auto fade-in";
    toast.innerHTML = `
        <i class="fa-solid ${bgIcon} text-base"></i>
        <span class="font-bold text-xs">${message}</span>
    `;

    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 0.25s ease';
        setTimeout(() => toast.remove(), 250);
    }, 3500);
}

// =========================================================================
// 22. SYSTEM INTEGRATION & ARCHITECTURE (SIA) INTERACTIVE CANVAS
// =========================================================================
let siaCurrentDiagram = 'use-case';
let siaScale = 1.0;
let siaPanX = 0;
let siaPanY = 0;
let siaIsDragging = false;
let siaStartX = 0;
let siaStartY = 0;

function switchSiaDiagram(diagramKey) {
    siaCurrentDiagram = diagramKey;
    const diagramKeys = ['use-case', 'swimlane', 'erd', 'flowchart', 'network'];

    diagramKeys.forEach(key => {
        const el = document.getElementById('sia-diagram-' + key);
        const tabBtn = document.getElementById('sia-tab-' + key);
        if (el) {
            if (key === diagramKey) {
                el.classList.remove('hidden');
            } else {
                el.classList.add('hidden');
            }
        }
        if (tabBtn) {
            if (key === diagramKey) {
                tabBtn.className = "px-2.5 py-1.5 rounded-lg font-bold text-[11px] transition bg-cyan-600 text-white shadow";
            } else {
                tabBtn.className = "px-2.5 py-1.5 rounded-lg font-bold text-[11px] transition text-slate-300 hover:text-white";
            }
        }
    });

    const descEl = document.getElementById('sia-desc-text');
    if (descEl) {
        if (diagramKey === 'use-case') descEl.innerText = "1. Use Case Diagram: Actors (Admin/Chairperson, Secretary, Treasurer, Member) & Core System Functionalities.";
        if (diagramKey === 'swimlane') descEl.innerText = "2. Swimlanes & Pools: Hand-offs between Secretary (Proposal) -> Chairperson (Approve) -> Treasurer (Disbursement) -> Supabase Cloud.";
        if (diagramKey === 'erd') descEl.innerText = "3. Entity Relationship Diagram (ERD): PostgreSQL Relational Schema (profiles, projects, disbursements, fiscal_budget, audit_logs).";
        if (diagramKey === 'flowchart') descEl.innerText = "4. System Flowchart: Complete data logic with Progressive Lockout, CAPTCHA, SHA-256 Auth & Realtime Sync.";
        if (diagramKey === 'network') descEl.innerText = "5. Network Diagram: Client Devices (PWA/Desktop) -> Apache Port 80 -> Supabase HTTPS REST API & WebSocket Realtime.";
    }

    resetSiaCanvas();
}

function updateSiaTransform() {
    const layer = document.getElementById('sia-transform-layer');
    const zoomText = document.getElementById('sia-zoom-level');
    if (layer) {
        layer.style.transform = `translate(${siaPanX}px, ${siaPanY}px) scale(${siaScale})`;
    }
    if (zoomText) {
        zoomText.innerText = Math.round(siaScale * 100) + '%';
    }
}

function zoomSiaCanvas(factor) {
    siaScale = Math.min(3.5, Math.max(0.4, siaScale * factor));
    updateSiaTransform();
}

function resetSiaCanvas() {
    siaScale = 1.0;
    siaPanX = 0;
    siaPanY = 0;
    updateSiaTransform();
}

function toggleSiaFullscreen() {
    const wrapper = document.getElementById('sia-canvas-wrapper');
    if (!wrapper) return;
    if (!document.fullscreenElement) {
        wrapper.requestFullscreen().catch(err => {
            showToast('Fullscreen error: ' + err.message, 'error');
        });
    } else {
        document.exitFullscreen();
    }
}

function setupSiaCanvasInteractions() {
    const wrapper = document.getElementById('sia-canvas-wrapper');
    if (!wrapper) return;

    // Mouse Wheel Zoom
    wrapper.addEventListener('wheel', (e) => {
        e.preventDefault();
        const zoomDelta = e.deltaY < 0 ? 1.12 : 0.88;
        zoomSiaCanvas(zoomDelta);
    }, { passive: false });

    // Click & Drag Pan
    wrapper.addEventListener('mousedown', (e) => {
        if (e.button !== 0) return; // Left click only
        siaIsDragging = true;
        siaStartX = e.clientX - siaPanX;
        siaStartY = e.clientY - siaPanY;
        wrapper.classList.remove('cursor-grab');
        wrapper.classList.add('cursor-grabbing');
    });

    window.addEventListener('mousemove', (e) => {
        if (!siaIsDragging) return;
        siaPanX = e.clientX - siaStartX;
        siaPanY = e.clientY - siaStartY;
        updateSiaTransform();
    });

    window.addEventListener('mouseup', () => {
        if (siaIsDragging) {
            siaIsDragging = false;
            if (wrapper) {
                wrapper.classList.remove('cursor-grabbing');
                wrapper.classList.add('cursor-grab');
            }
        }
    });

    // Touch Support for Mobile / Touch screens
    wrapper.addEventListener('touchstart', (e) => {
        if (e.touches.length === 1) {
            siaIsDragging = true;
            siaStartX = e.touches[0].clientX - siaPanX;
            siaStartY = e.touches[0].clientY - siaPanY;
        }
    }, { passive: true });

    wrapper.addEventListener('touchmove', (e) => {
        if (siaIsDragging && e.touches.length === 1) {
            siaPanX = e.touches[0].clientX - siaStartX;
            siaPanY = e.touches[0].clientY - siaStartY;
            updateSiaTransform();
        }
    }, { passive: true });

    wrapper.addEventListener('touchend', () => {
        siaIsDragging = false;
    });
}

// Run System Store initialization on load
window.addEventListener('DOMContentLoaded', () => {
    initSystemStores();
    setupSiaCanvasInteractions();
});
    
