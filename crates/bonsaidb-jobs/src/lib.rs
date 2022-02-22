use bonsaidb_core::schema::Schematic;

pub fn define_collections(schematic: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
    queue::define_collections(schematic)?;
    job::define_collections(schematic)?;

    Ok(())
}

pub mod job;
pub mod orchestrator;
pub mod queue;
mod schema;
pub mod worker;
