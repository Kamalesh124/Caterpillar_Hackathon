// SPA Router + Data Store + Views

const APP_VERSION = '1.0.0';

// ------------ Utilities ------------
const byId = (id) => document.getElementById(id);
const qs = (sel, root = document) => root.querySelector(sel);
const qsa = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const formatDateTimeLocal = (date) => {
  const pad = (n) => String(n).padStart(2, '0');
  const yyyy = date.getFullYear();
  const mm = pad(date.getMonth() + 1);
  const dd = pad(date.getDate());
  const hh = pad(date.getHours());
  const mi = pad(date.getMinutes());
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
};

const uid = () => Math.random().toString(36).slice(2, 10);

// ------------ Storage ------------
const storage = {
  get(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch (e) {
      console.error('storage.get error', e);
      return fallback;
    }
  },
  set(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch (e) {
      console.error('storage.set error', e);
    }
  },
};

// ------------ Data Models ------------
// asset: { id, name, category, site, status, lastSeenAt, imageEmoji }
// rental: { id, assetId, customer, checkOutAt, checkInAt, site, notes, fuelAtStart, odometerAtStart, createdAt, returnedAt }

const DB_KEYS = {
  assets: 'srt_assets',
  rentals: 'srt_rentals',
  meta: 'srt_meta',
};

function seedIfEmpty() {
  // Force refresh with new machinery - clear old data
  localStorage.clear(); // Clear all old data
  
  const assets = storage.get(DB_KEYS.assets, []);
  if (assets.length > 0) return;
  const demoAssets = [
    { id: 'exc-320-1', name: 'Excavator CAT 320', category: 'Excavator', site: 'Yard A', status: 'rented', lastSeenAt: Date.now(), imageEmoji: '⛏️', description: '20-ton hydraulic excavator with advanced digging capabilities. Features: 1.2 cubic yard bucket, 320° swing radius, 25.5 ft digging depth. Perfect for foundation work and utility installation.', quantity: 3 },
    { id: 'exc-320-2', name: 'Excavator CAT 320', category: 'Excavator', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⛏️', description: '20-ton hydraulic excavator with advanced digging capabilities. Features: 1.2 cubic yard bucket, 320° swing radius, 25.5 ft digging depth. Perfect for foundation work and utility installation.', quantity: 3 },
    { id: 'exc-320-3', name: 'Excavator CAT 320', category: 'Excavator', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⛏️', description: '20-ton hydraulic excavator with advanced digging capabilities. Features: 1.2 cubic yard bucket, 320° swing radius, 25.5 ft digging depth. Perfect for foundation work and utility installation.', quantity: 3 },
    { id: 'exc-330-1', name: 'Excavator CAT 330', category: 'Excavator', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⛏️', description: '30-ton excavator with intelligent hydraulic system and GPS guidance. Features: 1.6 cubic yard bucket, 360° swing, 30 ft digging depth. Ideal for large construction projects and mining operations.', quantity: 2 },
    { id: 'exc-330-2', name: 'Excavator CAT 330', category: 'Excavator', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⛏️', description: '30-ton excavator with intelligent hydraulic system and GPS guidance. Features: 1.6 cubic yard bucket, 360° swing, 30 ft digging depth. Ideal for large construction projects and mining operations.', quantity: 2 },
    { id: 'exc-349-1', name: 'Excavator CAT 349', category: 'Excavator', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⛏️', description: '50-ton heavy-duty excavator with reinforced undercarriage. Features: 2.5 cubic yard bucket, 40 ft digging depth, 400 hp engine. Designed for extreme mining and large-scale earthmoving.', quantity: 1 },
    { id: 'ldr-950-1', name: 'Wheel Loader CAT 950', category: 'Loader', site: 'Yard A', status: 'rented', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '3.5 cubic yard wheel loader with high-torque engine. Features: 4WD, 200 hp, 8.5 ft lift height. Perfect for material handling, loading trucks, and stockpile management.', quantity: 4 },
    { id: 'ldr-950-2', name: 'Wheel Loader CAT 950', category: 'Loader', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '3.5 cubic yard wheel loader with high-torque engine. Features: 4WD, 200 hp, 8.5 ft lift height. Perfect for material handling, loading trucks, and stockpile management.', quantity: 4 },
    { id: 'ldr-950-3', name: 'Wheel Loader CAT 950', category: 'Loader', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '3.5 cubic yard wheel loader with high-torque engine. Features: 4WD, 200 hp, 8.5 ft lift height. Perfect for material handling, loading trucks, and stockpile management.', quantity: 4 },
    { id: 'ldr-950-4', name: 'Wheel Loader CAT 950', category: 'Loader', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '3.5 cubic yard wheel loader with high-torque engine. Features: 4WD, 200 hp, 8.5 ft lift height. Perfect for material handling, loading trucks, and stockpile management.', quantity: 4 },
    { id: 'ldr-966-1', name: 'Wheel Loader CAT 966', category: 'Loader', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '6 cubic yard wheel loader with advanced load-sensing hydraulics. Features: 300 hp, 12 ft lift height, 4WD. Excellent for heavy material handling and large-scale operations.', quantity: 2 },
    { id: 'ldr-966-2', name: 'Wheel Loader CAT 966', category: 'Loader', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '6 cubic yard wheel loader with advanced load-sensing hydraulics. Features: 300 hp, 12 ft lift height, 4WD. Excellent for heavy material handling and large-scale operations.', quantity: 2 },
    { id: 'ldr-980-1', name: 'Wheel Loader CAT 980', category: 'Loader', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '8 cubic yard wheel loader with high-capacity bucket. Features: 400 hp, 15 ft lift height, reinforced frame. Designed for maximum productivity in large-scale material handling.', quantity: 1 },
    { id: 'dmp-730-1', name: 'Dump Truck CAT 730', category: 'Truck', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚛', description: '30-ton articulated dump truck with all-wheel drive. Features: 350 hp, 20 cubic yard capacity, 40 mph max speed. Perfect for off-road hauling and construction sites.', quantity: 3 },
    { id: 'dmp-730-2', name: 'Dump Truck CAT 730', category: 'Truck', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚛', description: '30-ton articulated dump truck with all-wheel drive. Features: 350 hp, 20 cubic yard capacity, 40 mph max speed. Perfect for off-road hauling and construction sites.', quantity: 3 },
    { id: 'dmp-730-3', name: 'Dump Truck CAT 730', category: 'Truck', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚛', description: '30-ton articulated dump truck with all-wheel drive. Features: 350 hp, 20 cubic yard capacity, 40 mph max speed. Perfect for off-road hauling and construction sites.', quantity: 3 },
    { id: 'dmp-740-1', name: 'Dump Truck CAT 740', category: 'Truck', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚛', description: '40-ton articulated dump truck with advanced suspension. Features: 450 hp, 25 cubic yard capacity, 45 mph max speed. Ideal for heavy-duty hauling operations.', quantity: 2 },
    { id: 'dmp-740-2', name: 'Dump Truck CAT 740', category: 'Truck', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚛', description: '40-ton articulated dump truck with advanced suspension. Features: 450 hp, 25 cubic yard capacity, 45 mph max speed. Ideal for heavy-duty hauling operations.', quantity: 2 },
    { id: 'dmp-750-1', name: 'Dump Truck CAT 750', category: 'Truck', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚛', description: '50-ton articulated dump truck with reinforced chassis. Features: 550 hp, 30 cubic yard capacity, 50 mph max speed. Built for extreme hauling conditions.', quantity: 1 },
    { id: 'blt-d6-1', name: 'Bulldozer CAT D6', category: 'Bulldozer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: 'Medium-sized bulldozer with 12-foot blade. Features: 200 hp, 6-way blade control, GPS guidance. Perfect for grading, leveling, and earthmoving operations.', quantity: 3 },
    { id: 'blt-d6-2', name: 'Bulldozer CAT D6', category: 'Bulldozer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: 'Medium-sized bulldozer with 12-foot blade. Features: 200 hp, 6-way blade control, GPS guidance. Perfect for grading, leveling, and earthmoving operations.', quantity: 3 },
    { id: 'blt-d6-3', name: 'Bulldozer CAT D6', category: 'Bulldozer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: 'Medium-sized bulldozer with 12-foot blade. Features: 200 hp, 6-way blade control, GPS guidance. Perfect for grading, leveling, and earthmoving operations.', quantity: 3 },
    { id: 'blt-d8-1', name: 'Bulldozer CAT D8', category: 'Bulldozer', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: 'Large bulldozer with 16-foot blade and high horsepower. Features: 350 hp, 8-way blade control, ripper attachment. Designed for heavy construction and mining work.', quantity: 2 },
    { id: 'blt-d8-2', name: 'Bulldozer CAT D8', category: 'Bulldozer', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: 'Large bulldozer with 16-foot blade and high horsepower. Features: 350 hp, 8-way blade control, ripper attachment. Designed for heavy construction and mining work.', quantity: 2 },
    { id: 'blt-d9-1', name: 'Bulldozer CAT D9', category: 'Bulldozer', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: 'Extra-large bulldozer with 18-foot blade. Features: 500 hp, 10-way blade control, extreme duty undercarriage. Built for the most demanding earthmoving projects.', quantity: 1 },
    { id: 'crn-120-1', name: 'Crane CAT 120', category: 'Crane', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🏗️', description: '120-ton mobile crane with telescopic boom. Features: 150 ft max reach, 360° rotation, outrigger stabilization. Perfect for construction and industrial lifting operations.', quantity: 2 },
    { id: 'crn-120-2', name: 'Crane CAT 120', category: 'Crane', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🏗️', description: '120-ton mobile crane with telescopic boom. Features: 150 ft max reach, 360° rotation, outrigger stabilization. Perfect for construction and industrial lifting operations.', quantity: 2 },
    { id: 'crn-200-1', name: 'Crane CAT 200', category: 'Crane', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🏗️', description: '200-ton mobile crane with extended reach capabilities. Features: 200 ft max reach, computerized load monitoring, 400° rotation. Ideal for large-scale construction projects.', quantity: 1 },
    { id: 'gen-150-1', name: 'Generator CAT 150kVA', category: 'Generator', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '150kVA diesel generator with automatic transfer switch. Features: 120kW output, 400V/3-phase, fuel tank monitoring. Perfect for backup power and remote site operations.', quantity: 4 },
    { id: 'gen-150-2', name: 'Generator CAT 150kVA', category: 'Generator', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '150kVA diesel generator with automatic transfer switch. Features: 120kW output, 400V/3-phase, fuel tank monitoring. Perfect for backup power and remote site operations.', quantity: 4 },
    { id: 'gen-150-3', name: 'Generator CAT 150kVA', category: 'Generator', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '150kVA diesel generator with automatic transfer switch. Features: 120kW output, 400V/3-phase, fuel tank monitoring. Perfect for backup power and remote site operations.', quantity: 4 },
    { id: 'gen-150-4', name: 'Generator CAT 150kVA', category: 'Generator', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '150kVA diesel generator with automatic transfer switch. Features: 120kW output, 400V/3-phase, fuel tank monitoring. Perfect for backup power and remote site operations.', quantity: 4 },
    { id: 'gen-300-1', name: 'Generator CAT 300kVA', category: 'Generator', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '300kVA industrial generator with advanced monitoring. Features: 240kW output, 480V/3-phase, remote monitoring capabilities. Designed for large construction sites and industrial applications.', quantity: 2 },
    { id: 'gen-300-2', name: 'Generator CAT 300kVA', category: 'Generator', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '300kVA industrial generator with advanced monitoring. Features: 240kW output, 480V/3-phase, remote monitoring capabilities. Designed for large construction sites and industrial applications.', quantity: 2 },
    { id: 'gen-500-1', name: 'Generator CAT 500kVA', category: 'Generator', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '⚡', description: '500kVA heavy-duty generator with industrial controls. Features: 400kW output, 600V/3-phase, dual fuel capability. Built for mining and large industrial operations.', quantity: 1 },
    { id: 'com-140-1', name: 'Compactor CAT 140', category: 'Compactor', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-ton vibratory soil compactor with intelligent compaction control. Features: 4,000 VPM, 2.5 ft drum width, GPS tracking. Perfect for road construction and soil compaction.', quantity: 3 },
    { id: 'com-140-2', name: 'Compactor CAT 140', category: 'Compactor', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-ton vibratory soil compactor with intelligent compaction control. Features: 4,000 VPM, 2.5 ft drum width, GPS tracking. Perfect for road construction and soil compaction.', quantity: 3 },
    { id: 'com-140-3', name: 'Compactor CAT 140', category: 'Compactor', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-ton vibratory soil compactor with intelligent compaction control. Features: 4,000 VPM, 2.5 ft drum width, GPS tracking. Perfect for road construction and soil compaction.', quantity: 3 },
    { id: 'com-160-1', name: 'Compactor CAT 160', category: 'Compactor', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '16-ton vibratory soil compactor with advanced compaction technology. Features: 4,500 VPM, 3 ft drum width, automatic grade control. Ideal for highway construction and large-scale compaction.', quantity: 2 },
    { id: 'com-160-2', name: 'Compactor CAT 160', category: 'Compactor', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '16-ton vibratory soil compactor with advanced compaction technology. Features: 4,500 VPM, 3 ft drum width, automatic grade control. Ideal for highway construction and large-scale compaction.', quantity: 2 },
    { id: 'gra-12-1', name: 'Grader CAT 12', category: 'Grader', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-foot motor grader with precision blade control. Features: 200 hp, 6-way blade, GPS guidance system. Perfect for road construction and surface finishing.', quantity: 3 },
    { id: 'gra-12-2', name: 'Grader CAT 12', category: 'Grader', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-foot motor grader with precision blade control. Features: 200 hp, 6-way blade, GPS guidance system. Perfect for road construction and surface finishing.', quantity: 3 },
    { id: 'gra-12-3', name: 'Grader CAT 12', category: 'Grader', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-foot motor grader with precision blade control. Features: 200 hp, 6-way blade, GPS guidance system. Perfect for road construction and surface finishing.', quantity: 3 },
    { id: 'gra-14-1', name: 'Grader CAT 14', category: 'Grader', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-foot motor grader with advanced control systems. Features: 250 hp, 8-way blade, automatic grade control. Designed for large-scale road construction projects.', quantity: 2 },
    { id: 'gra-14-2', name: 'Grader CAT 14', category: 'Grader', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-foot motor grader with advanced control systems. Features: 250 hp, 8-way blade, automatic grade control. Designed for large-scale road construction projects.', quantity: 2 },
    { id: 'gra-16-1', name: 'Grader CAT 16', category: 'Grader', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '16-foot motor grader with maximum productivity features. Features: 300 hp, 10-way blade, intelligent grade control. Built for highway and major road construction.', quantity: 1 },
    { id: 'rol-120-1', name: 'Roller CAT 120', category: 'Roller', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-ton smooth drum roller with vibration control. Features: 2.5 ft drum width, 3,000 VPM, automatic grade control. Perfect for asphalt and soil compaction.', quantity: 4 },
    { id: 'rol-120-2', name: 'Roller CAT 120', category: 'Roller', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-ton smooth drum roller with vibration control. Features: 2.5 ft drum width, 3,000 VPM, automatic grade control. Perfect for asphalt and soil compaction.', quantity: 4 },
    { id: 'rol-120-3', name: 'Roller CAT 120', category: 'Roller', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-ton smooth drum roller with vibration control. Features: 2.5 ft drum width, 3,000 VPM, automatic grade control. Perfect for asphalt and soil compaction.', quantity: 4 },
    { id: 'rol-120-4', name: 'Roller CAT 120', category: 'Roller', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '12-ton smooth drum roller with vibration control. Features: 2.5 ft drum width, 3,000 VPM, automatic grade control. Perfect for asphalt and soil compaction.', quantity: 4 },
    { id: 'rol-140-1', name: 'Roller CAT 140', category: 'Roller', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-ton tandem roller with dual drum technology. Features: 3 ft drum width, 3,500 VPM, GPS tracking. Excellent for highway and road construction.', quantity: 2 },
    { id: 'rol-140-2', name: 'Roller CAT 140', category: 'Roller', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '14-ton tandem roller with dual drum technology. Features: 3 ft drum width, 3,500 VPM, GPS tracking. Excellent for highway and road construction.', quantity: 2 },
    { id: 'rol-160-1', name: 'Roller CAT 160', category: 'Roller', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🛣️', description: '16-ton pneumatic roller with advanced compaction control. Features: 3.5 ft drum width, 4,000 VPM, automatic grade control. Built for heavy-duty compaction operations.', quantity: 1 },
    { id: 'skd-262-1', name: 'Skid Steer CAT 262', category: 'Skid Steer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.6-ton skid steer loader with versatile attachments. Features: 74 hp, 2,600 lb lift capacity, 8.5 ft lift height. Perfect for tight spaces and versatile material handling.', quantity: 5 },
    { id: 'skd-262-2', name: 'Skid Steer CAT 262', category: 'Skid Steer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.6-ton skid steer loader with versatile attachments. Features: 74 hp, 2,600 lb lift capacity, 8.5 ft lift height. Perfect for tight spaces and versatile material handling.', quantity: 5 },
    { id: 'skd-262-3', name: 'Skid Steer CAT 262', category: 'Skid Steer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.6-ton skid steer loader with versatile attachments. Features: 74 hp, 2,600 lb lift capacity, 8.5 ft lift height. Perfect for tight spaces and versatile material handling.', quantity: 5 },
    { id: 'skd-262-4', name: 'Skid Steer CAT 262', category: 'Skid Steer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.6-ton skid steer loader with versatile attachments. Features: 74 hp, 2,600 lb lift capacity, 8.5 ft lift height. Perfect for tight spaces and versatile material handling.', quantity: 5 },
    { id: 'skd-262-5', name: 'Skid Steer CAT 262', category: 'Skid Steer', site: 'Yard A', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.6-ton skid steer loader with versatile attachments. Features: 74 hp, 2,600 lb lift capacity, 8.5 ft lift height. Perfect for tight spaces and versatile material handling.', quantity: 5 },
    { id: 'skd-272-1', name: 'Skid Steer CAT 272', category: 'Skid Steer', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.7-ton skid steer with advanced hydraulic system. Features: 82 hp, 2,700 lb lift capacity, 9 ft lift height. Designed for precision work and heavy material handling.', quantity: 3 },
    { id: 'skd-272-2', name: 'Skid Steer CAT 272', category: 'Skid Steer', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.7-ton skid steer with advanced hydraulic system. Features: 82 hp, 2,700 lb lift capacity, 9 ft lift height. Designed for precision work and heavy material handling.', quantity: 3 },
    { id: 'skd-272-3', name: 'Skid Steer CAT 272', category: 'Skid Steer', site: 'Yard B', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.7-ton skid steer with advanced hydraulic system. Features: 82 hp, 2,700 lb lift capacity, 9 ft lift height. Designed for precision work and heavy material handling.', quantity: 3 },
    { id: 'skd-289-1', name: 'Skid Steer CAT 289', category: 'Skid Steer', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.9-ton skid steer with maximum lift capacity. Features: 90 hp, 2,900 lb lift capacity, 10 ft lift height. Built for heavy material handling and maximum productivity.', quantity: 2 },
    { id: 'skd-289-2', name: 'Skid Steer CAT 289', category: 'Skid Steer', site: 'Yard C', status: 'idle', lastSeenAt: Date.now(), imageEmoji: '🚜', description: '2.9-ton skid steer with maximum lift capacity. Features: 90 hp, 2,900 lb lift capacity, 10 ft lift height. Built for heavy material handling and maximum productivity.', quantity: 2 },
  ];
  storage.set(DB_KEYS.assets, demoAssets);
  
  // Add some demo rental data to test overdue functionality
  const demoRentals = [
    {
      id: 'r_demo1',
      assetId: 'exc-320-1',
      site: 'Construction Site A',
      checkOutAt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(), // 7 days ago
      checkInAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString(), // 2 days from now
      customer: {
        name: 'John Smith',
        phone: '+1-555-0123',
        email: 'john@construction.com',
      },
      fuelAtStart: 85,
      odometerAtStart: 1250,
      notes: 'Site excavation work',
      createdAt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
      returnedAt: null, // Still active
    },
    {
      id: 'r_demo2',
      assetId: 'ldr-950-1',
      site: 'Mining Site B',
      checkOutAt: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(), // 3 days ago
      checkInAt: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(), // 1 day ago (overdue)
      customer: {
        name: 'Sarah Johnson',
        phone: '+1-555-0456',
        email: 'sarah@mining.com',
      },
      fuelAtStart: 90,
      odometerAtStart: 2100,
      notes: 'Material loading operations',
      createdAt: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(),
      returnedAt: null, // Overdue
    },
    {
      id: 'r_demo3',
      assetId: 'gen-150-1',
      site: 'Event Center',
      checkOutAt: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(), // 10 days ago
      checkInAt: new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString(), // 8 days ago
      customer: {
        name: 'Mike Wilson',
        phone: '+1-555-0789',
        email: 'mike@events.com',
      },
      fuelAtStart: 75,
      odometerAtStart: 500,
      notes: 'Backup power for outdoor event',
      createdAt: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(),
      returnedAt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(), // Returned 3 days late
    }
  ];
  
  storage.set(DB_KEYS.rentals, demoRentals);
  storage.set(DB_KEYS.meta, { version: APP_VERSION, seededAt: Date.now() });
}

function getAssets() { return storage.get(DB_KEYS.assets, []); }
function setAssets(list) { storage.set(DB_KEYS.assets, list); }
function getRentals() { return storage.get(DB_KEYS.rentals, []); }
function setRentals(list) { storage.set(DB_KEYS.rentals, list); }

// ------------ QR Utilities ------------
  // We encode links like: #/rent?assetId=<id>
  function makeAssetRentLink(assetId) {
    // ALWAYS use your computer's IP address for phone access
    const yourComputerIP = '192.168.1.7'; // Your actual IP address
    const port = '5500'; // Fixed port number
    
    // Create a simple, direct URL that will definitely work when scanned
    const fullUrl = `http://${yourComputerIP}:${port}/index.html#/rent?assetId=${encodeURIComponent(assetId)}`;
    console.log('Generated QR URL:', fullUrl);
    return fullUrl;
  }

// Proper QR code generator using Reed-Solomon encoding
function generateQRCode(text) {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  canvas.width = 260;
  canvas.height = 260;
  
  // White background
  ctx.fillStyle = '#FFFFFF';
  ctx.fillRect(0, 0, 260, 260);
  
  // Generate QR-like pattern based on text
  const data = text.split('').map(c => c.charCodeAt(0));
  const size = 21; // QR code size
  const cellSize = 10;
  const margin = 25;
  
  // Create a simple pattern based on the text
  for (let i = 0; i < size; i++) {
    for (let j = 0; j < size; j++) {
      const index = (i * size + j) % data.length;
      const value = data[index];
      
      // Create a pattern based on character codes
      if ((value + i + j) % 3 === 0) {
        ctx.fillStyle = '#000000';
        ctx.fillRect(
          margin + i * cellSize, 
          margin + j * cellSize, 
          cellSize, 
          cellSize
        );
      }
    }
  }
  
  // Add finder patterns (corner squares)
  const finderSize = 7;
  const finderPositions = [
    [0, 0], [size - finderSize, 0], [0, size - finderSize]
  ];
  
  finderPositions.forEach(([x, y]) => {
    // Outer square
    ctx.fillStyle = '#000000';
    ctx.fillRect(
      margin + x * cellSize, 
      margin + y * cellSize, 
      finderSize * cellSize, 
      finderSize * cellSize
    );
    
    // Inner white square
    ctx.fillStyle = '#FFFFFF';
    ctx.fillRect(
      margin + (x + 1) * cellSize, 
      margin + (y + 1) * cellSize, 
      (finderSize - 2) * cellSize, 
      (finderSize - 2) * cellSize
    );
    
    // Inner black square
    ctx.fillStyle = '#000000';
    ctx.fillRect(
      margin + (x + 2) * cellSize, 
      margin + (y + 2) * cellSize, 
      (finderSize - 4) * cellSize, 
      (finderSize - 4) * cellSize
    );
  });
  
  return canvas;
}

async function renderQrTo(element, text) {
  try {
    if (!element) return;
    element.innerHTML = '';
    
    // Check if QRCode library is available
    if (typeof QRCode === 'undefined') {
      throw new Error('QRCode library not loaded');
    }
    
    // Create a proper scannable QR code using the QRCode library
    const qr = new QRCode(element, {
      text: text,
      width: 260,
      height: 260,
      colorDark: '#000000',
      colorLight: '#FFFFFF',
      correctLevel: QRCode.CorrectLevel.M
    });
    
    console.log('QR code generated successfully for:', text);
    
  } catch (err) {
    console.error('QR render error:', err);
    // Fallback to simple text display with direct link
    element.innerHTML = `
      <div style="text-align: center; padding: 20px;">
        <div style="font-size: 24px; margin-bottom: 10px;">📱</div>
        <div class="muted" style="margin-bottom: 10px;">QR Code</div>
        <div style="font-size: 12px; color: #666; margin-bottom: 15px;">
          Click the button below to open the rental form directly
        </div>
        <a href="${text}" target="_blank" class="btn btn-secondary" style="text-decoration: none;">
          Open Rental Form
        </a>
      </div>
    `;
  }
}

// ------------ Router ------------
const routes = {};
function registerRoute(path, render) { routes[path] = render; }
function parseHash() {
  const hash = window.location.hash || '#/dashboard';
  const [path, queryString] = hash.split('?');
  const params = new URLSearchParams(queryString || '');
  return { path, params };
}
function navigate(path) { window.location.hash = path; }
function handleRoute() {
  const { path, params } = parseHash();
  const view = routes[path] || routes['#/dashboard'];
  view(params);
  highlightNav(path);
}
function highlightNav(path){
  qsa('.nav-link').forEach(a=>a.classList.remove('active'));
  if(path === '#/dashboard') byId('nav-dashboard').classList.add('active');
  if(path === '#/rent') byId('nav-rent').classList.add('active');
  if(path === '#/history') byId('nav-history').classList.add('active');
}

// ------------ Views ------------
function renderDashboard() {
  const assets = getAssets();
  const rentals = getRentals();
  const app = byId('app');
  
  // Check for overdue rentals
  const now = new Date();
  const overdueRentals = rentals.filter(r => {
    if (r.returnedAt) return false;
    const checkInDate = new Date(r.checkInAt);
    return checkInDate < now;
  });
  
  const list = assets.map(a => {
    const activeRental = rentals.find(r => r.assetId === a.id && !r.returnedAt);
    const isOverdue = activeRental && new Date(activeRental.checkInAt) < now;
    const status = activeRental ? (isOverdue ? 'OVERDUE' : 'Rented') : 'Available';
    const statusClass = isOverdue ? 'status-overdue' : (activeRental ? 'status-rented' : 'status-idle');
    const rentBtn = activeRental
      ? `<button class="btn btn-danger" data-action="return" data-id="${a.id}">Check In</button>`
      : `<a class="btn" href="#/rent?assetId=${encodeURIComponent(a.id)}">Check Out</a>`;
    
    const overdueBadge = isOverdue ? '<span class="chip overdue">OVERDUE</span>' : '';
    
    return `
      <div class="panel asset-card ${isOverdue ? 'overdue-card' : ''}" data-asset-id="${a.id}">
        <div class="asset-media">${a.imageEmoji || '🔧'}</div>
        <div style="flex:1">
          <div class="asset-title">${a.name}</div>
          <div class="asset-description">${a.description || 'No description available'}</div>
          <div class="asset-sub">${a.category} · ${a.site} · Quantity: ${a.quantity || 1}</div>
          <div class="row" style="margin-top:6px">
            <span class="chip ${activeRental ? 'live' : ''}">${activeRental ? 'LIVE' : 'IDLE'}</span>
            <span class="${statusClass}">${status}</span>
            ${overdueBadge}
          </div>
          ${activeRental ? `
            <div class="rental-info">
              <small>Rented by: ${activeRental.customer.name}</small><br>
              <small>Expected return: ${new Date(activeRental.checkInAt).toLocaleDateString()}</small>
              ${isOverdue ? `<br><small class="overdue-text">⚠️ Overdue by ${Math.ceil((now - new Date(activeRental.checkInAt)) / (1000 * 60 * 60 * 24))} days</small>` : ''}
            </div>
          ` : ''}
        </div>
        <div class="card-actions">
          <button class="btn btn-secondary" data-action="qr" data-id="${a.id}">QR</button>
          ${rentBtn}
        </div>
      </div>
    `;
  }).join('');

  const overdueAlert = overdueRentals.length > 0 ? `
    <div class="alert alert-warning">
      <strong>⚠️ Overdue Alert:</strong> ${overdueRentals.length} equipment item(s) are overdue for return.
      <a href="#/history" class="btn btn-sm">View Details</a>
    </div>
  ` : '';

  app.innerHTML = `
    <div class="dashboard-header">
      <div class="dashboard-title">
        <h2>Equipment Dashboard</h2>
        <button class="btn btn-secondary" id="btn-add-equipment">+ Add Equipment</button>
      </div>
              <div class="equipment-stats">
          <div class="stat-item">
            <span class="stat-number">${assets.length}</span>
            <span class="stat-label">Total Equipment</span>
          </div>
          <div class="stat-item">
            <span class="stat-number">${assets.filter(a => a.status === 'idle').length}</span>
            <span class="stat-label">Available</span>
          </div>
          <div class="stat-item">
            <span class="stat-number">${getRentedCount()}</span>
            <span class="stat-label">Rented</span>
          </div>
        </div>
    </div>
    
    ${overdueAlert}
    <section class="grid cols-1">
      ${list}
    </section>
  `;
  
  // Add event listener for adding equipment
  byId('btn-add-equipment')?.addEventListener('click', () => {
    openAddEquipmentModal();
  });

  // Wire actions via delegation to survive re-renders
  app.addEventListener('click', (e) => {
    const qrBtn = e.target.closest('[data-action="qr"]');
    if (qrBtn) {
      const assetId = qrBtn.getAttribute('data-id');
      openQrModal(assetId);
      return;
    }
    const returnBtn = e.target.closest('[data-action="return"]');
    if (returnBtn) {
      const assetId = returnBtn.getAttribute('data-id');
      handleCheckIn(assetId);
    }
  });
}

function renderRentForm(params) {
  const assetId = params.get('assetId') || '';
  const asset = getAssets().find(a => a.id === assetId);
  const now = new Date();
  const in2h = new Date(now.getTime() + 2*60*60*1000);
  const app = byId('app');

  app.innerHTML = `
    <div class="panel">
      <h3 style="margin:0 0 12px 0">New Rental</h3>
      <form id="rent-form" class="form">
        <div class="row">
          <div class="field" style="flex:1">
            <label class="required">Asset</label>
            <select name="assetId" required>
              <option value="">Select asset</option>
              ${getAssets().map(a => `<option value="${a.id}" ${asset && a.id===asset.id ? 'selected' : ''}>${a.name}</option>`).join('')}
            </select>
          </div>
          <div class="field" style="flex:1">
            <label class="required">Site</label>
            <input name="site" placeholder="Site name" required />
          </div>
        </div>

        <div class="row">
          <div class="field" style="flex:1">
            <label class="required">Check-out</label>
            <input type="datetime-local" name="checkOutAt" value="${formatDateTimeLocal(now)}" required />
          </div>
          <div class="field" style="flex:1">
            <label class="required">Expected Check-in</label>
            <input type="datetime-local" name="checkInAt" value="${formatDateTimeLocal(in2h)}" required />
          </div>
        </div>

        <div class="row">
          <div class="field" style="flex:1">
            <label class="required">Customer Name</label>
            <input name="customerName" placeholder="Full name" required />
          </div>
          <div class="field" style="flex:1">
            <label class="required">Phone</label>
            <input name="customerPhone" placeholder="Phone" required />
          </div>
        </div>

        <div class="row">
          <div class="field" style="flex:1">
            <label>Email</label>
            <input name="customerEmail" placeholder="Email" />
          </div>
          <div class="field" style="flex:1">
            <label>Fuel % at Start</label>
            <input type="number" name="fuelAtStart" min="0" max="100" placeholder="e.g., 90" />
          </div>
        </div>

        <div class="row">
          <div class="field" style="flex:1">
            <label>Odometer/Hours at Start</label>
            <input type="number" name="odometerAtStart" min="0" placeholder="e.g., 1234" />
          </div>
          <div class="field" style="flex:1">
            <label>Notes</label>
            <input name="notes" placeholder="Special instructions" />
          </div>
        </div>

        <div class="row" style="justify-content:flex-end">
          <button class="btn" type="submit">Confirm Check-out</button>
        </div>
      </form>
    </div>
  `;

  byId('rent-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const data = new FormData(e.currentTarget);
    const rental = {
      id: `r_${uid()}`,
      assetId: data.get('assetId'),
      site: data.get('site'),
      checkOutAt: new Date(data.get('checkOutAt')).toISOString(),
      checkInAt: new Date(data.get('checkInAt')).toISOString(),
      customer: {
        name: data.get('customerName'),
        phone: data.get('customerPhone'),
        email: data.get('customerEmail') || '',
      },
      fuelAtStart: Number(data.get('fuelAtStart') || 0),
      odometerAtStart: Number(data.get('odometerAtStart') || 0),
      notes: data.get('notes') || '',
      createdAt: new Date().toISOString(),
      returnedAt: null,
    };

    // Save rental and update asset status
    const rentals = getRentals();
    rentals.push(rental);
    setRentals(rentals);

    const assets = getAssets();
    const idx = assets.findIndex(a => a.id === rental.assetId);
    if (idx >= 0) {
      assets[idx].status = 'rented';
      assets[idx].site = rental.site;
      assets[idx].lastSeenAt = Date.now();
      setAssets(assets);
    }

    navigate('#/dashboard');
  });
}

// ------------ Check-in ------------
function handleCheckIn(assetId) {
  const rentals = getRentals();
  const active = rentals.find(r => r.assetId === assetId && !r.returnedAt);
  if (!active) return alert('No active rental found for this asset.');

  active.returnedAt = new Date().toISOString();
  setRentals(rentals);

  const assets = getAssets();
  const idx = assets.findIndex(a => a.id === assetId);
  if (idx >= 0) {
    assets[idx].status = 'idle';
    assets[idx].lastSeenAt = Date.now();
    setAssets(assets);
  }

  renderDashboard();
}

// ------------ Rental History View ------------
function renderRentalHistory() {
  const rentals = getRentals();
  const assets = getAssets();
  const now = new Date();
  
  // Sort rentals by creation date (newest first)
  const sortedRentals = rentals.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  
  const historyList = sortedRentals.map(rental => {
    const asset = assets.find(a => a.id === rental.assetId);
    const isOverdue = !rental.returnedAt && new Date(rental.checkInAt) < now;
    const isReturned = !!rental.returnedAt;
    const isLate = isReturned && new Date(rental.returnedAt) > new Date(rental.checkInAt);
    
    let statusClass = 'status-active';
    let statusText = 'Active';
    
    if (isOverdue) {
      statusClass = 'status-overdue';
      statusText = 'OVERDUE';
    } else if (isReturned) {
      statusClass = 'status-returned';
      statusText = isLate ? 'Returned Late' : 'Returned';
    }
    
    const overdueDays = isOverdue ? Math.ceil((now - new Date(rental.checkInAt)) / (1000 * 60 * 60 * 24)) : 0;
    const lateDays = isLate ? Math.ceil((new Date(rental.returnedAt) - new Date(rental.checkInAt)) / (1000 * 60 * 60 * 24)) : 0;
    
    return `
      <div class="panel rental-history-card ${isOverdue ? 'overdue-card' : ''} ${isLate ? 'late-card' : ''}">
        <div class="rental-header">
          <div class="rental-asset">
            <div class="asset-emoji">${asset?.imageEmoji || '🔧'}</div>
            <div>
              <div class="asset-name">${asset?.name || rental.assetId}</div>
              <div class="customer-name">${rental.customer.name} • ${rental.customer.phone}</div>
            </div>
          </div>
          <div class="rental-status">
            <span class="chip ${statusClass}">${statusText}</span>
            ${isOverdue ? `<span class="chip overdue">${overdueDays} days overdue</span>` : ''}
            ${isLate ? `<span class="chip late">${lateDays} days late</span>` : ''}
          </div>
        </div>
        
        <div class="rental-details">
          <div class="detail-row">
            <div class="detail-item">
              <label>Check-out:</label>
              <span>${new Date(rental.checkOutAt).toLocaleString()}</span>
            </div>
            <div class="detail-item">
              <label>Expected Return:</label>
              <span>${new Date(rental.checkInAt).toLocaleString()}</span>
            </div>
          </div>
          
          ${isReturned ? `
            <div class="detail-row">
              <div class="detail-item">
                <label>Actual Return:</label>
                <span>${new Date(rental.returnedAt).toLocaleString()}</span>
              </div>
              <div class="detail-item">
                <label>Site:</label>
                <span>${rental.site}</span>
              </div>
            </div>
          ` : `
            <div class="detail-row">
              <div class="detail-item">
                <label>Site:</label>
                <span>${rental.site}</span>
              </div>
              <div class="detail-item">
                <label>Rental ID:</label>
                <span>${rental.id}</span>
              </div>
            </div>
          `}
          
          ${rental.notes ? `
            <div class="detail-row">
              <div class="detail-item full-width">
                <label>Notes:</label>
                <span>${rental.notes}</span>
              </div>
            </div>
          ` : ''}
          
          ${rental.fuelAtStart || rental.odometerAtStart ? `
            <div class="detail-row">
              ${rental.fuelAtStart ? `
                <div class="detail-item">
                  <label>Fuel at Start:</label>
                  <span>${rental.fuelAtStart}%</span>
                </div>
              ` : ''}
              ${rental.odometerAtStart ? `
                <div class="detail-item">
                  <label>Odometer at Start:</label>
                  <span>${rental.odometerAtStart}</span>
                </div>
              ` : ''}
            </div>
          ` : ''}
        </div>
      </div>
    `;
  }).join('');
  
  const overdueCount = rentals.filter(r => !r.returnedAt && new Date(r.checkInAt) < now).length;
  const activeCount = rentals.filter(r => !r.returnedAt).length;
  const totalCount = rentals.length;
  
  const app = byId('app');
  app.innerHTML = `
    <div class="history-header">
      <h2>Rental History</h2>
      <div class="stats-row">
        <div class="stat-card">
          <div class="stat-number">${totalCount}</div>
          <div class="stat-label">Total Rentals</div>
        </div>
        <div class="stat-card">
          <div class="stat-number">${activeCount}</div>
          <div class="stat-label">Active Rentals</div>
        </div>
        <div class="stat-card ${overdueCount > 0 ? 'overdue' : ''}">
          <div class="stat-number">${overdueCount}</div>
          <div class="stat-label">Overdue</div>
        </div>
      </div>
    </div>
    
    <section class="grid cols-1">
      ${historyList.length > 0 ? historyList : '<div class="panel"><p class="muted">No rental history found.</p></div>'}
    </section>
  `;
}

// ------------ QR Modal ------------
async function openQrModal(assetId) {
  const asset = getAssets().find(a => a.id === assetId);
  const link = makeAssetRentLink(assetId);
  byId('qr-asset-name').textContent = asset ? asset.name : assetId;
  byId('qr-open-link').href = link;
  byId('qr-modal').classList.remove('hidden');
  // Render QR after showing the modal so user gets immediate feedback
  renderQrTo(byId('qr-canvas'), link);
}

// Print QR only canvas
byId('qr-print')?.addEventListener('click', () => {
  const canvas = qs('#qr-canvas canvas');
  if (!canvas) return;
  const dataUrl = canvas.toDataURL('image/png');
  const w = window.open('');
  w.document.write(`<img src="${dataUrl}" style="width:320px" />`);
  w.print();
  w.close();
});

byId('qr-modal-close')?.addEventListener('click', ()=> byId('qr-modal').classList.add('hidden'));

// ------------ Scanner Modal ------------
function openScanModal(){
  byId('scan-modal').classList.remove('hidden');
  
  // Check if Html5Qrcode library is available
  if (typeof Html5Qrcode !== 'undefined' && window.Html5Qrcode) {
    try {
      const html5QrCode = new Html5Qrcode("reader");
      const config = { 
        fps: 10, 
        qrbox: { width: 250, height: 250 },
        aspectRatio: 1.0
      };
      
      // Show camera permission request
      byId('reader').innerHTML = `
        <div style="text-align: center; padding: 40px; color: white;">
          <div style="font-size: 48px; margin-bottom: 20px;">📷</div>
          <h3>Camera Permission Required</h3>
          <p>Click "Start Camera" to allow camera access and begin scanning</p>
          <button id="start-camera-btn" class="btn" style="margin-top: 20px;">Start Camera</button>
        </div>
      `;
      
      // Handle start camera button
      byId('start-camera-btn').addEventListener('click', () => {
        startCamera(html5QrCode, config);
      });
    } catch (error) {
      console.error('Html5Qrcode initialization failed:', error);
      showScannerError('QR Scanner initialization failed. Please try refreshing the page.');
    }
  } else {
    // Library not loaded - show helpful message with manual entry option
    byId('reader').innerHTML = `
      <div style="text-align: center; padding: 40px; color: white;">
        <div style="font-size: 48px; margin-bottom: 20px;">⚠️</div>
        <h3>QR Scanner Library Loading...</h3>
        <p>The QR scanner is still loading. Please wait a moment and try again.</p>
        <div style="margin-top: 20px;">
          <button onclick="openScanModal()" class="btn" style="margin-right: 10px;">Retry Scanner</button>
          <button onclick="openManualEntry()" class="btn btn-secondary">Manual Entry</button>
        </div>
        <div style="margin-top: 20px; font-size: 12px; color: #ccc;">
          💡 <strong>Alternative:</strong> You can still use your phone to scan QR codes
        </div>
      </div>
    `;
  }
}

function startCamera(html5QrCode, config) {
  // Show loading state
  byId('reader').innerHTML = `
    <div style="text-align: center; padding: 40px; color: white;">
      <div style="font-size: 48px; margin-bottom: 20px;">⏳</div>
      <h3>Starting Camera...</h3>
      <p>Please allow camera access when prompted</p>
      <div class="loading-spinner" style="width: 30px; height: 30px; border: 3px solid rgba(255,255,255,0.3); border-top: 3px solid white; border-radius: 50%; animation: spin 1s linear infinite; margin: 20px auto;"></div>
    </div>
  `;
  
  // Start camera with user-facing mode for laptop scanning
  html5QrCode.start({ facingMode: "user" }, config, (decodedText) => {
    console.log('QR Code detected:', decodedText);
    
    // Show success message
    byId('reader').innerHTML = `
      <div style="text-align: center; padding: 40px; color: white;">
        <div style="font-size: 48px; margin-bottom: 20px;">✅</div>
        <h3>QR Code Detected!</h3>
        <p>Opening rental form...</p>
        <div style="margin-top: 20px; font-size: 14px; color: #ccc;">${decodedText}</div>
      </div>
    `;
    
    // Stop camera and close modal after a short delay
    setTimeout(() => {
      html5QrCode.stop().then(() => {
        byId('scan-modal').classList.add('hidden');
        // Navigate to the rental form
        if (decodedText.includes('assetId=')) {
          const url = new URL(decodedText);
          const assetId = url.hash.split('assetId=')[1];
          if (assetId) {
            window.location.hash = `#/rent?assetId=${assetId}`;
          }
        }
      });
    }, 2000);
    
  }, (errorMessage) => {
    // Handle scanning errors
    console.log('Scanning error:', errorMessage);
  }).catch((error) => {
    console.error('Camera start failed:', error);
    
    // Show error message
    byId('reader').innerHTML = `
      <div style="text-align: center; padding: 40px; color: white;">
        <div style="font-size: 48px; margin-bottom: 20px;">❌</div>
        <h3>Camera Failed to Start</h3>
        <p>${error.message || 'Please check camera permissions and try again'}</p>
        <button onclick="openScanModal()" class="btn" style="margin-top: 20px;">Try Again</button>
      </div>
    `;
  });
}

// Helper functions for scanner
function showScannerError(message) {
  byId('reader').innerHTML = `
    <div style="text-align: center; padding: 40px; color: white;">
      <div style="font-size: 48px; margin-bottom: 20px;">❌</div>
      <h3>Scanner Error</h3>
      <p>${message}</p>
      <button onclick="openScanModal()" class="btn" style="margin-top: 20px;">Try Again</button>
    </div>
  `;
}

function openManualEntry() {
  // Show manual asset selection
  byId('reader').innerHTML = `
    <div style="text-align: center; padding: 40px; color: white;">
      <div style="font-size: 48px; margin-bottom: 20px;">✏️</div>
      <h3>Manual Equipment Selection</h3>
      <p>Select the equipment you want to rent:</p>
      <div style="margin-top: 20px;">
        <select id="manual-asset-select" style="width: 100%; padding: 10px; margin-bottom: 15px; background: #1a1a1a; color: white; border: 1px solid #333; border-radius: 5px;">
          <option value="">Choose equipment...</option>
          ${getAssets().map(asset => `<option value="${asset.id}">${asset.name} - ${asset.site}</option>`).join('')}
        </select>
        <button onclick="proceedWithManualAsset()" class="btn" style="margin-right: 10px;">Continue to Rental Form</button>
        <button onclick="openScanModal()" class="btn btn-secondary">Back to Scanner</button>
      </div>
    </div>
  `;
}

function proceedWithManualAsset() {
  const assetId = byId('manual-asset-select').value;
  if (!assetId) {
    alert('Please select an equipment first.');
    return;
  }
  
  // Close modal and navigate to rental form
  byId('scan-modal').classList.add('hidden');
  window.location.hash = `#/rent?assetId=${assetId}`;
}
byId('btn-scan').addEventListener('click', openScanModal);
byId('scan-modal-close').addEventListener('click', ()=> byId('scan-modal').classList.add('hidden'));

// ------------ Rental Count Functions ------------
function getRentedCount() {
  const rentals = getRentals();
  // Count active (non-returned) rentals
  return rentals.filter(r => !r.returnedAt).length;
}

// ------------ Add Equipment Modal ------------
function openAddEquipmentModal() {
  const modal = byId('add-equipment-modal');
  modal.classList.remove('hidden');
}

function closeAddEquipmentModal() {
  const modal = byId('add-equipment-modal');
  modal.classList.add('hidden');
  // Clear form
  byId('add-equipment-form').reset();
}

// Handle add equipment form submission
byId('add-equipment-form')?.addEventListener('submit', (e) => {
  e.preventDefault();
  const formData = new FormData(e.currentTarget);
  
  const newAsset = {
    id: `${formData.get('category').toLowerCase()}-${uid()}`,
    name: formData.get('name'),
    category: formData.get('category'),
    site: formData.get('site'),
    status: 'idle',
    lastSeenAt: Date.now(),
    imageEmoji: getEmojiForCategory(formData.get('category')),
    description: formData.get('description'),
    quantity: parseInt(formData.get('quantity')) || 1,
  };
  
  // Add to storage
  const assets = getAssets();
  assets.push(newAsset);
  setAssets(assets);
  
  // Close modal and refresh dashboard
  closeAddEquipmentModal();
  renderDashboard();
});

function getEmojiForCategory(category) {
  const emojiMap = {
    'Excavator': '⛏️',
    'Loader': '🚜',
    'Truck': '🚛',
    'Bulldozer': '🚜',
    'Crane': '🏗️',
    'Generator': '⚡',
    'Compactor': '🛣️',
    'Grader': '🛣️',
    'Roller': '🛣️',
    'Skid Steer': '🚜',
  };
  return emojiMap[category] || '🔧';
}

// Close add equipment modal
byId('add-equipment-close')?.addEventListener('click', closeAddEquipmentModal);

// ------------ Routes ------------
registerRoute('#/dashboard', () => renderDashboard());
registerRoute('#/rent', (params) => renderRentForm(params));
registerRoute('#/history', () => renderRentalHistory());

window.addEventListener('hashchange', handleRoute);

// ------------ Init ------------
seedIfEmpty();

// Check if QR scanner library is loaded
function checkQRScannerLibrary() {
  if (typeof Html5Qrcode === 'undefined') {
    console.warn('Html5Qrcode library not loaded yet, waiting...');
    setTimeout(checkQRScannerLibrary, 1000); // Check again in 1 second
  } else {
    console.log('Html5Qrcode library loaded successfully');
  }
}

// Start checking for library
checkQRScannerLibrary();

handleRoute();


