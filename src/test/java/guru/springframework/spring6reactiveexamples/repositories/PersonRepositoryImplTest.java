package guru.springframework.spring6reactiveexamples.repositories;

import guru.springframework.spring6reactiveexamples.domain.Person;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PersonRepositoryImplTest {

    PersonRepository repo = new PersonRepositoryImpl();

    @Test
    void testMonoByIdBlock() { // Come NON fare un test con Mono
        Mono<Person> personMono = repo.getById(1);

        Person person = personMono.block();

        assert person != null;

        System.out.println(person);
    }

    @Test
    void testGetByIdSubscriber() {
        Mono<Person> personMono = repo.getById(1);

        personMono.subscribe(System.out::println);
    }

    @Test
    void testMapOperation() {
        Mono<Person> personMono = repo.getById(1);

        personMono.map(Person::getFirstName).subscribe(System.out::println);
    }

    @Test
    void testFluxBlockFirst() { // Come NON fare un test con Flux
        Flux<Person> personFlux = repo.findAll();

        Person person = personFlux.blockFirst();

        System.out.println(person);
    }

    /*
     La method reference (`System.out::println`) è utile quando vogliamo eseguire un'operazione direttamente sull'oggetto emesso dal Flux,
     senza doverlo esplicitare con una lambda.

     Ad esempio, `personFlux.subscribe(System.out::println)` è equivalente a `personFlux.subscribe(person -> System.out.println(person))`,
     ma più conciso e leggibile.

     Il metodo `println` di `System.out` accetta un oggetto e chiama automaticamente il suo `toString()`, quindi possiamo usare
     direttamente `System.out::println` invece di scrivere una lambda esplicita.
    */

    @Test
    void testFluxSubscriber() {
        Flux<Person> personFlux = repo.findAll();

        personFlux.subscribe(System.out::println);

        personFlux.subscribe(person -> System.out.println(person.toString()));
    }

    @Test
    void testFluxMap() {
        Flux<Person> personFlux = repo.findAll();

        personFlux.map(Person::getFirstName).subscribe(System.out::println);

        personFlux.map(person -> person.getFirstName()).subscribe(firstName -> System.out.println(firstName));
    }

    @Test
    void testFluxToList() {
        Flux<Person> personFlux = repo.findAll();

        Mono<List<Person>> listMono = personFlux.collectList();

        listMono.subscribe(System.out::println);

        listMono.subscribe(list -> list.forEach(System.out::println));

        listMono.subscribe(list -> list.forEach(person -> System.out.println(person.getFirstName())));
    }

    @Test
    void testFilterOnName() {
        repo.findAll().filter(person -> person.getFirstName().startsWith("J")).subscribe(person -> System.out.println(person.getFirstName()));
    }

    @Test
    void testGetByName() {
        Mono<Person> personMono = repo.findAll().filter(person -> person.getFirstName().equals("Fiona")).next();

        personMono.subscribe(person -> System.out.println(person.getFirstName()));
    }

    @Test
    void testGetById() {
        Mono<Person> personMono = repo.getById(3);

        assertEquals(Boolean.TRUE, personMono.hasElement().block());
    }

    @Test
    void testGetByIdNotFound() {
        Mono<Person> personMono = repo.getById(6);

        assertEquals(Boolean.FALSE, personMono.hasElement().block());
    }

    @Test
    void testFindPersonByIdNotFound() {
        Flux<Person> personFlux = repo.findAll();

        final Integer id = 8; // Non possiamo mutare variabili in un reactive stream, quindi le variabili saranno definite final!

        /*
        Il metodo .next() emette solo il primo valore emesso dal Flux in un nuovo Mono. Se è chiamato su un Flux vuoto, emette un Mono vuoto.
        Utilizziamo quindi .single(), che si aspetta ed emette un singolo valore dal Flux,
        oppure una NoSuchElementException per un Flux vuoto, oppure IndexOutOfBoundsException per una source con più di un elemento!
         */
        Mono<Person> personMono = personFlux.filter(person -> person.getId().equals(id)).single()
                .doOnError(throwable -> {
                    System.out.println("Error occurred in the flux");
                    System.out.println(throwable.toString());
                });

        /*
         Se rimuoviamo completamente il blocco `.subscribe()`, nessun errore verrà emesso, né il Mono verrà eseguito.

         Questo accade perché Project Reactor segue un modello lazy (a esecuzione pigra):
         un flusso reattivo (Flux/Mono) non viene eseguito fino a quando non c'è un subscriber.

         In questo caso, `personMono` è definito, ma non essendoci nessun `.subscribe()`, il flusso non viene attivato
         e quindi nessuna operazione viene effettivamente eseguita.

         Di conseguenza:
         - Il filtro `.filter(person -> person.getId().equals(id))` non viene applicato.
         - Il `.single()` non viene valutato, quindi non viene mai generata un'eccezione (NoSuchElementException in caso di Mono vuoto).
         - Il `.doOnError(...)` non viene mai eseguito.

         Senza la subscribe, tutto rimane dichiarativo, ma non viene mai eseguito.
        */
        personMono.subscribe(person -> { // La statement lambda può essere sostituita con expression lambda (no bracket, stessa riga).
                                                // Tutta la lambda può essere sostituita da "System.out::println", essendo che stiamo effettuando il print dell'oggetto risultante dal subscribe
            System.out.println(person.toString());
        }, throwable -> {
            System.out.println("Error occurred in the mono");
            System.out.println(throwable.toString());
        });
    }

}